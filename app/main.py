import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field, model_validator
from PIL import Image

from app.db import get_conn, init_db

APP_SECRET = os.getenv("APP_SECRET", "dev-secret-change-me")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "86400"))
PHOTO_DIR = Path(__file__).resolve().parent.parent / "storage" / "photos"
PHOTO_DIR.mkdir(parents=True, exist_ok=True)


class RegisterRequest(BaseModel):
    email: Optional[str] = None
    phone: Optional[str] = None
    password: str = Field(min_length=8, max_length=128)

    @model_validator(mode="after")
    def validate_identity(self):
        if not self.email and not self.phone:
            raise ValueError("email or phone is required")
        return self


class LoginRequest(BaseModel):
    email: Optional[str] = None
    phone: Optional[str] = None
    password: str


class ContactIn(BaseModel):
    local_id: str = Field(min_length=1, max_length=128)
    display_name: str = ""
    given_name: str = ""
    family_name: str = ""
    phone_numbers: List[Dict[str, Any]] = Field(default_factory=list)
    email_addresses: List[Dict[str, Any]] = Field(default_factory=list)
    postal_addresses: List[Dict[str, Any]] = Field(default_factory=list)
    organization: str = ""
    job_title: str = ""
    notes: str = ""
    source_device_id: str = ""


class BatchUploadRequest(BaseModel):
    contacts: List[ContactIn] = Field(min_items=1, max_items=500)


class SyncChange(BaseModel):
    op: str = Field(pattern="^(upsert|delete)$")
    local_id: str = Field(min_length=1, max_length=128)
    display_name: str = ""
    given_name: str = ""
    family_name: str = ""
    phone_numbers: List[Dict[str, Any]] = Field(default_factory=list)
    email_addresses: List[Dict[str, Any]] = Field(default_factory=list)
    postal_addresses: List[Dict[str, Any]] = Field(default_factory=list)
    organization: str = ""
    job_title: str = ""
    notes: str = ""
    source_device_id: str = ""


class SyncRequest(BaseModel):
    last_sync_time: str
    local_changes: List[SyncChange] = Field(default_factory=list, max_items=1000)
    device_id: str = Field(min_length=1, max_length=128)


class SyncAckRequest(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    acked_until: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"


app = FastAPI(title="CloudSyncContacts v0.2", version="0.2.0")
init_db()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_or_400(value: str, field_name: str) -> datetime:
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid {field_name}") from exc


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("utf-8")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("utf-8"))


def create_token(user_id: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": user_id,
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "exp": int(datetime.now(timezone.utc).timestamp()) + TOKEN_TTL_SECONDS,
        "jti": secrets.token_hex(8),
    }
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(APP_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


def verify_token(token: str) -> Dict[str, Any]:
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="invalid token") from exc

    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    expected = hmac.new(APP_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    actual = _b64url_decode(sig_b64)
    if not hmac.compare_digest(expected, actual):
        raise HTTPException(status_code=401, detail="invalid token signature")

    payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    if int(payload.get("exp", 0)) < int(datetime.now(timezone.utc).timestamp()):
        raise HTTPException(status_code=401, detail="token expired")
    return payload


def password_hash(password: str, salt: Optional[str] = None) -> str:
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 200000)
    return f"{salt}${digest.hex()}"


def verify_password(password: str, hashed: str) -> bool:
    salt, digest = hashed.split("$", 1)
    candidate = password_hash(password, salt)
    return hmac.compare_digest(candidate, f"{salt}${digest}")


def contact_fingerprint(contact: ContactIn) -> str:
    data = {
        "display_name": contact.display_name,
        "given_name": contact.given_name,
        "family_name": contact.family_name,
        "phone_numbers": contact.phone_numbers,
        "email_addresses": contact.email_addresses,
        "postal_addresses": contact.postal_addresses,
        "organization": contact.organization,
        "job_title": contact.job_title,
        "notes": contact.notes,
        "source_device_id": contact.source_device_id,
    }
    body = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def sync_change_fingerprint(change: SyncChange) -> str:
    data = {
        "display_name": change.display_name,
        "given_name": change.given_name,
        "family_name": change.family_name,
        "phone_numbers": change.phone_numbers,
        "email_addresses": change.email_addresses,
        "postal_addresses": change.postal_addresses,
        "organization": change.organization,
        "job_title": change.job_title,
        "notes": change.notes,
        "source_device_id": change.source_device_id,
    }
    body = json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def serialize_contact(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "contact_id": row["contact_id"],
        "local_id": row["local_id"],
        "display_name": row["display_name"],
        "given_name": row["given_name"],
        "family_name": row["family_name"],
        "phone_numbers": json.loads(row["phone_numbers"] or "[]"),
        "email_addresses": json.loads(row["email_addresses"] or "[]"),
        "postal_addresses": json.loads(row["postal_addresses"] or "[]"),
        "organization": row["organization"],
        "job_title": row["job_title"],
        "notes": row["notes"],
        "photo_uri": row["photo_uri"],
        "source_device_id": row["source_device_id"],
        "version": row["version"],
        "sync_status": row["sync_status"],
        "hash": row["hash"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "deleted_at": row["deleted_at"],
    }


def get_current_user_id(authorization: str = Header(default="")) -> str:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    payload = verify_token(token)
    return payload["sub"]


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/auth/register", response_model=TokenResponse)
def register(body: RegisterRequest) -> TokenResponse:
    user_id = str(uuid.uuid4())
    created_at = now_iso()
    hashed = password_hash(body.password)

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO users (user_id, email, phone, password_hash, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, body.email, body.phone, hashed, created_at),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="user already exists") from exc
    finally:
        conn.close()

    return TokenResponse(access_token=create_token(user_id))


@app.post("/api/v1/auth/login", response_model=TokenResponse)
def login(body: LoginRequest) -> TokenResponse:
    if not body.email and not body.phone:
        raise HTTPException(status_code=400, detail="email or phone is required")

    conn = get_conn()
    try:
        cur = conn.cursor()
        if body.email:
            cur.execute("SELECT user_id, password_hash FROM users WHERE email = ?", (body.email,))
        else:
            cur.execute("SELECT user_id, password_hash FROM users WHERE phone = ?", (body.phone,))
        row = cur.fetchone()
    finally:
        conn.close()

    if row is None or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="invalid credentials")

    return TokenResponse(access_token=create_token(row["user_id"]))


@app.post("/api/v1/contacts/batch")
def batch_upload(body: BatchUploadRequest, user_id: str = Depends(get_current_user_id)) -> Dict[str, Any]:
    created = 0
    updated = 0
    now = now_iso()

    conn = get_conn()
    try:
        cur = conn.cursor()
        for item in body.contacts:
            hash_value = contact_fingerprint(item)
            cur.execute(
                "SELECT contact_id, hash, version FROM contacts WHERE user_id = ? AND local_id = ?",
                (user_id, item.local_id),
            )
            existing = cur.fetchone()

            json_phone = json.dumps(item.phone_numbers, ensure_ascii=False)
            json_email = json.dumps(item.email_addresses, ensure_ascii=False)
            json_addr = json.dumps(item.postal_addresses, ensure_ascii=False)

            if existing is None:
                cur.execute(
                    """
                    INSERT INTO contacts (
                        contact_id, user_id, display_name, given_name, family_name,
                        phone_numbers, email_addresses, postal_addresses,
                        organization, job_title, notes, photo_uri,
                        source_device_id, local_id, version, sync_status,
                        hash, created_at, updated_at, deleted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        str(uuid.uuid4()),
                        user_id,
                        item.display_name,
                        item.given_name,
                        item.family_name,
                        json_phone,
                        json_email,
                        json_addr,
                        item.organization,
                        item.job_title,
                        item.notes,
                        "",
                        item.source_device_id,
                        item.local_id,
                        1,
                        "synced",
                        hash_value,
                        now,
                        now,
                    ),
                )
                created += 1
            elif existing["hash"] != hash_value:
                cur.execute(
                    """
                    UPDATE contacts
                    SET display_name = ?, given_name = ?, family_name = ?,
                        phone_numbers = ?, email_addresses = ?, postal_addresses = ?,
                        organization = ?, job_title = ?, notes = ?,
                        source_device_id = ?, version = ?, sync_status = ?, hash = ?,
                        updated_at = ?, deleted_at = NULL
                    WHERE contact_id = ?
                    """,
                    (
                        item.display_name,
                        item.given_name,
                        item.family_name,
                        json_phone,
                        json_email,
                        json_addr,
                        item.organization,
                        item.job_title,
                        item.notes,
                        item.source_device_id,
                        int(existing["version"]) + 1,
                        "synced",
                        hash_value,
                        now,
                        existing["contact_id"],
                    ),
                )
                updated += 1

        conn.commit()
    finally:
        conn.close()

    return {
        "created": created,
        "updated": updated,
        "skipped": len(body.contacts) - created - updated,
        "total": len(body.contacts),
    }


@app.get("/api/v1/contacts")
def list_contacts(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    offset = (page - 1) * page_size

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM contacts WHERE user_id = ? AND deleted_at IS NULL", (user_id,))
        total = int(cur.fetchone()["total"])

        cur.execute(
            """
            SELECT *
            FROM contacts
            WHERE user_id = ? AND deleted_at IS NULL
            ORDER BY updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (user_id, page_size, offset),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        items.append(serialize_contact(row))

    return {"page": page, "page_size": page_size, "total": total, "items": items}


@app.post("/api/v1/sync")
def sync_contacts(body: SyncRequest, user_id: str = Depends(get_current_user_id)) -> Dict[str, Any]:
    last_sync_time = parse_iso_or_400(body.last_sync_time, "last_sync_time")
    now = now_iso()
    conflicts: List[Dict[str, Any]] = []
    local_applied = {"created": 0, "updated": 0, "deleted": 0, "skipped": 0}

    conn = get_conn()
    try:
        cur = conn.cursor()
        for change in body.local_changes:
            cur.execute(
                "SELECT * FROM contacts WHERE user_id = ? AND local_id = ?",
                (user_id, change.local_id),
            )
            existing = cur.fetchone()
            is_server_newer = False
            if existing is not None:
                is_server_newer = parse_iso_or_400(existing["updated_at"], "updated_at") > last_sync_time

            if change.op == "delete":
                if existing is None or existing["deleted_at"] is not None:
                    local_applied["skipped"] += 1
                    continue
                if is_server_newer:
                    conflicts.append(
                        {
                            "local_id": change.local_id,
                            "type": "delete-modify",
                            "server": serialize_contact(existing),
                        }
                    )
                    continue
                cur.execute(
                    """
                    UPDATE contacts
                    SET deleted_at = ?, updated_at = ?, sync_status = ?, source_device_id = ?
                    WHERE contact_id = ?
                    """,
                    (now, now, "synced", body.device_id, existing["contact_id"]),
                )
                local_applied["deleted"] += 1
                continue

            hash_value = sync_change_fingerprint(change)
            if existing is None:
                cur.execute(
                    """
                    INSERT INTO contacts (
                        contact_id, user_id, display_name, given_name, family_name,
                        phone_numbers, email_addresses, postal_addresses,
                        organization, job_title, notes, photo_uri,
                        source_device_id, local_id, version, sync_status,
                        hash, created_at, updated_at, deleted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    (
                        str(uuid.uuid4()),
                        user_id,
                        change.display_name,
                        change.given_name,
                        change.family_name,
                        json.dumps(change.phone_numbers, ensure_ascii=False),
                        json.dumps(change.email_addresses, ensure_ascii=False),
                        json.dumps(change.postal_addresses, ensure_ascii=False),
                        change.organization,
                        change.job_title,
                        change.notes,
                        "",
                        change.source_device_id or body.device_id,
                        change.local_id,
                        1,
                        "synced",
                        hash_value,
                        now,
                        now,
                    ),
                )
                local_applied["created"] += 1
                continue

            # If server has newer and changed content, defer to v0.3 conflict flow.
            if is_server_newer and existing["hash"] != hash_value and existing["deleted_at"] is None:
                conflicts.append(
                    {
                        "local_id": change.local_id,
                        "type": "value-conflict",
                        "server": serialize_contact(existing),
                    }
                )
                continue

            if existing["hash"] == hash_value and existing["deleted_at"] is None:
                local_applied["skipped"] += 1
                continue

            cur.execute(
                """
                UPDATE contacts
                SET display_name = ?, given_name = ?, family_name = ?,
                    phone_numbers = ?, email_addresses = ?, postal_addresses = ?,
                    organization = ?, job_title = ?, notes = ?,
                    source_device_id = ?, version = ?, sync_status = ?, hash = ?,
                    updated_at = ?, deleted_at = NULL
                WHERE contact_id = ?
                """,
                (
                    change.display_name,
                    change.given_name,
                    change.family_name,
                    json.dumps(change.phone_numbers, ensure_ascii=False),
                    json.dumps(change.email_addresses, ensure_ascii=False),
                    json.dumps(change.postal_addresses, ensure_ascii=False),
                    change.organization,
                    change.job_title,
                    change.notes,
                    change.source_device_id or body.device_id,
                    int(existing["version"]) + 1,
                    "synced",
                    hash_value,
                    now,
                    existing["contact_id"],
                ),
            )
            local_applied["updated"] += 1

        cur.execute(
            """
            SELECT *
            FROM contacts
            WHERE user_id = ?
              AND updated_at > ?
              AND (source_device_id IS NULL OR source_device_id != ?)
            ORDER BY updated_at ASC
            """,
            (user_id, last_sync_time.isoformat(), body.device_id),
        )
        server_rows = cur.fetchall()
        server_changes = []
        for row in server_rows:
            payload = serialize_contact(row)
            payload["op"] = "delete" if row["deleted_at"] else "upsert"
            server_changes.append(payload)

        conn.commit()
    finally:
        conn.close()

    return {
        "server_changes": server_changes,
        "conflicts": conflicts,
        "local_applied": local_applied,
        "sync_time": now,
    }


@app.get("/api/v1/sync/changes")
def get_sync_changes(
    since: str = Query(...),
    device_id: Optional[str] = Query(default=None),
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    since_time = parse_iso_or_400(since, "since")
    conn = get_conn()
    try:
        cur = conn.cursor()
        if device_id:
            cur.execute(
                """
                SELECT *
                FROM contacts
                WHERE user_id = ?
                  AND updated_at > ?
                  AND (source_device_id IS NULL OR source_device_id != ?)
                ORDER BY updated_at ASC
                """,
                (user_id, since_time.isoformat(), device_id),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM contacts
                WHERE user_id = ? AND updated_at > ?
                ORDER BY updated_at ASC
                """,
                (user_id, since_time.isoformat()),
            )
        rows = cur.fetchall()
    finally:
        conn.close()

    changes = []
    for row in rows:
        item = serialize_contact(row)
        item["op"] = "delete" if row["deleted_at"] else "upsert"
        changes.append(item)
    return {"changes": changes, "since": since_time.isoformat(), "count": len(changes)}


@app.post("/api/v1/sync/ack")
def sync_ack(body: SyncAckRequest, user_id: str = Depends(get_current_user_id)) -> Dict[str, str]:
    acked_until = parse_iso_or_400(body.acked_until, "acked_until").isoformat()
    created_at = now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sync_ack_log (ack_id, user_id, device_id, acked_until, created_at) VALUES (?, ?, ?, ?, ?)",
            (str(uuid.uuid4()), user_id, body.device_id, acked_until, created_at),
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "acknowledged", "acked_until": acked_until}


@app.post("/api/v1/contacts/{contact_id}/photo")
async def upload_photo(
    contact_id: str,
    request: Request,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, str]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT contact_id FROM contacts WHERE user_id = ? AND contact_id = ? AND deleted_at IS NULL",
            (user_id, contact_id),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="contact not found")

        raw = await request.body()
        if not raw:
            raise HTTPException(status_code=400, detail="empty request body")

        try:
            image = Image.open(BytesIO(raw)).convert("RGB")
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid image format") from exc

        out = BytesIO()
        quality = 90
        image.save(out, format="JPEG", quality=quality, optimize=True)
        while out.tell() > 512 * 1024 and quality > 30:
            quality -= 10
            out = BytesIO()
            image.save(out, format="JPEG", quality=quality, optimize=True)

        if out.tell() > 512 * 1024:
            raise HTTPException(status_code=413, detail="image too large after compression")

        file_name = f"{contact_id}.jpg"
        file_path = PHOTO_DIR / file_name
        file_path.write_bytes(out.getvalue())
        uri = f"/storage/photos/{file_name}"

        cur.execute(
            "UPDATE contacts SET photo_uri = ?, updated_at = ? WHERE contact_id = ?",
            (uri, now_iso(), contact_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {"photo_uri": uri}
