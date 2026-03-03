import base64
import difflib
import hashlib
import hmac
import itertools
import json
import os
import re
import secrets
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, model_validator
from PIL import Image

from app.db import get_conn, init_db, Cursor, Row
from app.cache import cache_get, cache_set, cache_delete

APP_SECRET = os.getenv("APP_SECRET", "dev-secret-change-me")
TOKEN_TTL_SECONDS = int(os.getenv("TOKEN_TTL_SECONDS", "86400"))
REFRESH_TOKEN_TTL_SECONDS = int(os.getenv("REFRESH_TOKEN_TTL_SECONDS", str(30 * 24 * 3600)))
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


class ResolveConflictRequest(BaseModel):
    strategy: str = Field(pattern="^(keep_local|keep_server|manual_merge)$")
    merged_contact: Optional[SyncChange] = None
    device_id: str = Field(min_length=1, max_length=128)


class RollbackRequest(BaseModel):
    history_id: str = Field(min_length=1)
    device_id: str = Field(min_length=1, max_length=128)


class DedupeMergeRequest(BaseModel):
    local_ids: List[str] = Field(min_items=2, max_items=20)
    device_id: str = Field(min_length=1, max_length=128)


class DedupeIgnoreRequest(BaseModel):
    local_id_a: str = Field(min_length=1, max_length=128)
    local_id_b: str = Field(min_length=1, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: Optional[str] = None
    token_type: str = "Bearer"


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=16)


app = FastAPI(title="CloudSyncContacts v1.0", version="1.0.0")
init_db()
METRICS: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0.0, "error_count": 0.0, "total_ms": 0.0})


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        route = request.scope.get("route")
        if route and hasattr(route, "path"):
            key = f"{request.method} {route.path}"
        else:
            key = f"{request.method} {request.url.path}"
        METRICS[key]["count"] += 1.0
        METRICS[key]["total_ms"] += elapsed_ms
        if status_code >= 400:
            METRICS[key]["error_count"] += 1.0


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


def create_token(user_id: str, token_type: str = "access", ttl_seconds: Optional[int] = None) -> str:
    ttl = ttl_seconds if ttl_seconds is not None else TOKEN_TTL_SECONDS
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": user_id,
        "typ": token_type,
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "exp": int(datetime.now(timezone.utc).timestamp()) + ttl,
        "jti": secrets.token_hex(8),
    }
    header_b64 = _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(APP_SECRET.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


def verify_token(token: str) -> Dict[str, Any]:
    cache_key = f"tok:{token[-16:]}"
    cached = cache_get(cache_key)
    if cached:
        payload = json.loads(cached)
        if int(payload.get("exp", 0)) < int(datetime.now(timezone.utc).timestamp()):
            cache_delete(cache_key)
            raise HTTPException(status_code=401, detail="token expired")
        return payload

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

    remaining = int(payload.get("exp", 0)) - int(datetime.now(timezone.utc).timestamp())
    ttl = min(60, max(1, remaining))
    cache_set(cache_key, json.dumps(payload), ttl_seconds=ttl)
    return payload


def token_digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def issue_refresh_token(cur: Cursor, user_id: str, created_at: str) -> str:
    refresh_token = create_token(user_id, token_type="refresh", ttl_seconds=REFRESH_TOKEN_TTL_SECONDS)
    cur.execute(
        """
        INSERT INTO refresh_tokens (token_id, user_id, token_hash, expires_at, revoked, created_at)
        VALUES (?, ?, ?, ?, 0, ?)
        """,
        (
            str(uuid.uuid4()),
            user_id,
            token_digest(refresh_token),
            datetime.fromtimestamp(
                verify_token(refresh_token)["exp"],
                tz=timezone.utc,
            ).isoformat(),
            created_at,
        ),
    )
    return refresh_token


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


def serialize_contact(row: Row) -> Dict[str, Any]:
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


def row_to_sync_change(row: Row, op_override: Optional[str] = None) -> Dict[str, Any]:
    payload = {
        "op": op_override or ("delete" if row["deleted_at"] else "upsert"),
        "local_id": row["local_id"],
        "display_name": row["display_name"] or "",
        "given_name": row["given_name"] or "",
        "family_name": row["family_name"] or "",
        "phone_numbers": json.loads(row["phone_numbers"] or "[]"),
        "email_addresses": json.loads(row["email_addresses"] or "[]"),
        "postal_addresses": json.loads(row["postal_addresses"] or "[]"),
        "organization": row["organization"] or "",
        "job_title": row["job_title"] or "",
        "notes": row["notes"] or "",
        "source_device_id": row["source_device_id"] or "",
    }
    return payload


def write_contact_history(cur: Cursor, row: Row, user_id: str, created_at: str) -> None:
    snapshot = serialize_contact(row)
    cur.execute(
        """
        INSERT INTO contact_history (history_id, user_id, contact_id, version, snapshot, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            user_id,
            row["contact_id"],
            int(row["version"]),
            json.dumps(snapshot, ensure_ascii=False),
            created_at,
        ),
    )


def insert_conflict(
    cur: Cursor,
    user_id: str,
    local_id: str,
    contact_id: Optional[str],
    conflict_type: str,
    local_payload: Dict[str, Any],
    server_payload: Dict[str, Any],
    created_at: str,
) -> Dict[str, Any]:
    conflict_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO conflict_log (
            conflict_id, user_id, local_id, contact_id, conflict_type,
            local_payload, server_payload, status, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?)
        """,
        (
            conflict_id,
            user_id,
            local_id,
            contact_id,
            conflict_type,
            json.dumps(local_payload, ensure_ascii=False),
            json.dumps(server_payload, ensure_ascii=False),
            created_at,
        ),
    )
    return {
        "conflict_id": conflict_id,
        "local_id": local_id,
        "type": conflict_type,
        "local": local_payload,
        "server": server_payload,
        "status": "open",
        "created_at": created_at,
    }


def normalize_phone(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("86") and len(digits) > 11:
        digits = digits[2:]
    if digits.startswith("0") and len(digits) > 10:
        digits = digits[1:]
    return digits


def phone_set(contact: Dict[str, Any]) -> set:
    values = set()
    for item in contact.get("phone_numbers", []):
        raw = str(item.get("value", ""))
        norm = normalize_phone(raw)
        if norm:
            values.add(norm)
    return values


def email_set(contact: Dict[str, Any]) -> set:
    values = set()
    for item in contact.get("email_addresses", []):
        raw = str(item.get("value", "")).strip().lower()
        if raw:
            values.add(raw)
    return values


def name_similarity(a: str, b: str) -> int:
    if not a or not b:
        return 0
    ratio = difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
    return int(ratio * 100)


def auxiliary_score(a: Dict[str, Any], b: Dict[str, Any]) -> int:
    org_a = (a.get("organization") or "").strip().lower()
    org_b = (b.get("organization") or "").strip().lower()
    if not org_a or not org_b or org_a != org_b:
        return 0
    job_sim = name_similarity(a.get("job_title", ""), b.get("job_title", ""))
    return 70 if job_sim < 50 else 100


def dedupe_score(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    phone_score = 100 if phone_set(a) & phone_set(b) else 0
    email_score = 100 if email_set(a) & email_set(b) else 0
    name_score = name_similarity(a.get("display_name", ""), b.get("display_name", ""))
    if name_score < 85:
        name_score = 0
    aux_score = auxiliary_score(a, b)
    total = int(0.4 * phone_score + 0.3 * email_score + 0.2 * name_score + 0.1 * aux_score)
    # Align with spec acceptance: same normalized phone should be flagged as suspected duplicate.
    if phone_score == 100 and total < 75:
        total = 75
    return {
        "total": total,
        "phone_score": phone_score,
        "email_score": email_score,
        "name_score": name_score,
        "aux_score": aux_score,
    }


def pair_key(a: str, b: str) -> str:
    x, y = sorted([a, b])
    return f"{x}|{y}"


def more_complete_name(a: str, b: str) -> str:
    return a if len((a or "").strip()) >= len((b or "").strip()) else b


def union_unique_dicts(left: List[Dict[str, Any]], right: List[Dict[str, Any]], key_field: str) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for item in left + right:
        key = str(item.get(key_field, "")).strip().lower()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def merge_two_contacts(primary: Dict[str, Any], secondary: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(primary)
    merged["display_name"] = more_complete_name(primary.get("display_name", ""), secondary.get("display_name", ""))
    merged["given_name"] = more_complete_name(primary.get("given_name", ""), secondary.get("given_name", ""))
    merged["family_name"] = more_complete_name(primary.get("family_name", ""), secondary.get("family_name", ""))
    merged["phone_numbers"] = union_unique_dicts(primary.get("phone_numbers", []), secondary.get("phone_numbers", []), "value")
    merged["email_addresses"] = union_unique_dicts(primary.get("email_addresses", []), secondary.get("email_addresses", []), "value")
    merged["postal_addresses"] = union_unique_dicts(primary.get("postal_addresses", []), secondary.get("postal_addresses", []), "value")
    merged["organization"] = primary.get("organization") or secondary.get("organization") or ""
    merged["job_title"] = primary.get("job_title") or secondary.get("job_title") or ""
    p_notes = (primary.get("notes") or "").strip()
    s_notes = (secondary.get("notes") or "").strip()
    if p_notes and s_notes and p_notes != s_notes:
        merged["notes"] = f"{p_notes}\\n---\\n{s_notes}"
    else:
        merged["notes"] = p_notes or s_notes
    merged["photo_uri"] = primary.get("photo_uri") or secondary.get("photo_uri") or ""
    return merged


def get_current_user_id(authorization: str = Header(default="")) -> str:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    payload = verify_token(token)
    if payload.get("typ") not in (None, "access"):
        raise HTTPException(status_code=401, detail="invalid token type")
    return payload["sub"]


def write_audit(
    cur: Cursor,
    action: str,
    user_id: Optional[str] = None,
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO audit_log (audit_id, user_id, action, target_type, target_id, metadata, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(uuid.uuid4()),
            user_id,
            action,
            target_type,
            target_id,
            json.dumps(metadata or {}, ensure_ascii=False),
            now_iso(),
        ),
    )


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
        refresh_token = issue_refresh_token(cur, user_id, created_at)
        write_audit(cur, "auth.register", user_id=user_id, target_type="user", target_id=user_id)
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="user already exists") from exc
    finally:
        conn.close()

    return TokenResponse(access_token=create_token(user_id, token_type="access"), refresh_token=refresh_token)


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
    user_id = row["user_id"]
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("UPDATE refresh_tokens SET revoked = 1 WHERE user_id = ? AND revoked = 0", (user_id,))
        refresh_token = issue_refresh_token(cur, user_id, now_iso())
        write_audit(cur, "auth.login", user_id=user_id, target_type="user", target_id=user_id)
        conn.commit()
    finally:
        conn.close()

    return TokenResponse(access_token=create_token(user_id, token_type="access"), refresh_token=refresh_token)


@app.post("/api/v1/auth/refresh", response_model=TokenResponse)
def refresh_access_token(body: RefreshRequest) -> TokenResponse:
    payload = verify_token(body.refresh_token)
    if payload.get("typ") != "refresh":
        raise HTTPException(status_code=401, detail="invalid refresh token")
    user_id = payload["sub"]
    digest = token_digest(body.refresh_token)
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM refresh_tokens WHERE user_id = ? AND token_hash = ? AND revoked = 0",
            (user_id, digest),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=401, detail="refresh token revoked or not found")
        if parse_iso_or_400(row["expires_at"], "expires_at") < datetime.now(timezone.utc):
            raise HTTPException(status_code=401, detail="refresh token expired")

        cur.execute("UPDATE refresh_tokens SET revoked = 1 WHERE token_id = ?", (row["token_id"],))
        new_refresh_token = issue_refresh_token(cur, user_id, now_iso())
        write_audit(cur, "auth.refresh", user_id=user_id, target_type="user", target_id=user_id)
        conn.commit()
    finally:
        conn.close()

    return TokenResponse(access_token=create_token(user_id, token_type="access"), refresh_token=new_refresh_token)


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
                "SELECT * FROM contacts WHERE user_id = ? AND local_id = ?",
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
                write_contact_history(cur, existing, user_id, now)
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

        write_audit(
            cur,
            "contacts.batch_upload",
            user_id=user_id,
            target_type="contact",
            metadata={"created": created, "updated": updated, "total": len(body.contacts)},
        )
        conn.commit()
        cache_delete(f"cnt:{user_id}")
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
        cnt_key = f"cnt:{user_id}"
        cached_total = cache_get(cnt_key)
        if cached_total is not None:
            total = int(cached_total)
        else:
            cur.execute("SELECT COUNT(*) AS total FROM contacts WHERE user_id = ? AND deleted_at IS NULL", (user_id,))
            total = int(cur.fetchone()["total"])
            cache_set(cnt_key, str(total), ttl_seconds=30)

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
            local_payload = change.model_dump()
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
                        insert_conflict(
                            cur=cur,
                            user_id=user_id,
                            local_id=change.local_id,
                            contact_id=existing["contact_id"],
                            conflict_type="delete-modify",
                            local_payload=local_payload,
                            server_payload=row_to_sync_change(existing),
                            created_at=now,
                        )
                    )
                    continue
                write_contact_history(cur, existing, user_id, now)
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
                    insert_conflict(
                        cur=cur,
                        user_id=user_id,
                        local_id=change.local_id,
                        contact_id=existing["contact_id"],
                        conflict_type="value-conflict",
                        local_payload=local_payload,
                        server_payload=row_to_sync_change(existing),
                        created_at=now,
                    )
                )
                continue

            if existing["hash"] == hash_value and existing["deleted_at"] is None:
                local_applied["skipped"] += 1
                continue

            write_contact_history(cur, existing, user_id, now)
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
        cache_delete(f"cnt:{user_id}")
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
        write_audit(
            cur,
            "sync.ack",
            user_id=user_id,
            target_type="device",
            target_id=body.device_id,
            metadata={"acked_until": acked_until},
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "acknowledged", "acked_until": acked_until}


@app.get("/api/v1/conflicts")
def list_conflicts(
    status: str = Query(default="open", pattern="^(open|resolved|all)$"),
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        if status == "all":
            cur.execute(
                "SELECT * FROM conflict_log WHERE user_id = ? ORDER BY created_at DESC",
                (user_id,),
            )
        else:
            cur.execute(
                "SELECT * FROM conflict_log WHERE user_id = ? AND status = ? ORDER BY created_at DESC",
                (user_id, status),
            )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        items.append(
            {
                "conflict_id": row["conflict_id"],
                "local_id": row["local_id"],
                "contact_id": row["contact_id"],
                "type": row["conflict_type"],
                "local": json.loads(row["local_payload"]),
                "server": json.loads(row["server_payload"]),
                "status": row["status"],
                "created_at": row["created_at"],
                "resolved_at": row["resolved_at"],
            }
        )
    return {"count": len(items), "items": items}


@app.post("/api/v1/conflicts/{conflict_id}/resolve")
def resolve_conflict(
    conflict_id: str,
    body: ResolveConflictRequest,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    resolved_at = now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM conflict_log WHERE conflict_id = ? AND user_id = ? AND status = 'open'",
            (conflict_id, user_id),
        )
        conflict = cur.fetchone()
        if conflict is None:
            raise HTTPException(status_code=404, detail="open conflict not found")

        local_payload = json.loads(conflict["local_payload"])
        server_payload = json.loads(conflict["server_payload"])
        contact_id = conflict["contact_id"]
        local_id = conflict["local_id"]

        if contact_id:
            cur.execute(
                "SELECT * FROM contacts WHERE contact_id = ? AND user_id = ?",
                (contact_id, user_id),
            )
            existing = cur.fetchone()
        else:
            existing = None

        if body.strategy == "keep_local":
            final_payload = local_payload
        elif body.strategy == "keep_server":
            final_payload = server_payload
        else:
            if body.merged_contact is None:
                raise HTTPException(status_code=400, detail="merged_contact required for manual_merge")
            final_payload = body.merged_contact.model_dump()

        op = final_payload.get("op", "upsert")
        if op == "delete":
            if existing is not None:
                write_contact_history(cur, existing, user_id, resolved_at)
                cur.execute(
                    """
                    UPDATE contacts
                    SET deleted_at = ?, updated_at = ?, source_device_id = ?, sync_status = ?
                    WHERE contact_id = ?
                    """,
                    (resolved_at, resolved_at, body.device_id, "synced", existing["contact_id"]),
                )
        else:
            change = SyncChange(**final_payload)
            hash_value = sync_change_fingerprint(change)
            if existing is None:
                cur.execute(
                    """
                    INSERT INTO contacts (
                        contact_id, user_id, display_name, given_name, family_name,
                        phone_numbers, email_addresses, postal_addresses,
                        organization, job_title, notes, photo_uri, source_device_id,
                        local_id, version, sync_status, hash, created_at, updated_at, deleted_at
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
                        local_id,
                        1,
                        "synced",
                        hash_value,
                        resolved_at,
                        resolved_at,
                    ),
                )
            else:
                write_contact_history(cur, existing, user_id, resolved_at)
                cur.execute(
                    """
                    UPDATE contacts
                    SET display_name = ?, given_name = ?, family_name = ?,
                        phone_numbers = ?, email_addresses = ?, postal_addresses = ?,
                        organization = ?, job_title = ?, notes = ?, source_device_id = ?,
                        version = ?, sync_status = ?, hash = ?, updated_at = ?, deleted_at = NULL
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
                        resolved_at,
                        existing["contact_id"],
                    ),
                )

        cur.execute(
            """
            UPDATE conflict_log
            SET status = 'resolved', resolved_payload = ?, resolved_at = ?
            WHERE conflict_id = ?
            """,
            (json.dumps(final_payload, ensure_ascii=False), resolved_at, conflict_id),
        )
        write_audit(
            cur,
            "conflict.resolve",
            user_id=user_id,
            target_type="conflict",
            target_id=conflict_id,
            metadata={"strategy": body.strategy},
        )
        conn.commit()
        cache_delete(f"cnt:{user_id}")
    finally:
        conn.close()

    return {"status": "resolved", "conflict_id": conflict_id, "resolved_at": resolved_at}


@app.get("/api/v1/contacts/{contact_id}/history")
def contact_history(contact_id: str, user_id: str = Depends(get_current_user_id)) -> Dict[str, Any]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT history_id, version, snapshot, created_at
            FROM contact_history
            WHERE user_id = ? AND contact_id = ?
            ORDER BY created_at DESC
            """,
            (user_id, contact_id),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    items = []
    for row in rows:
        items.append(
            {
                "history_id": row["history_id"],
                "version": row["version"],
                "snapshot": json.loads(row["snapshot"]),
                "created_at": row["created_at"],
            }
        )
    return {"count": len(items), "items": items}


@app.post("/api/v1/contacts/{contact_id}/rollback")
def rollback_contact(
    contact_id: str,
    body: RollbackRequest,
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    now = now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM contacts WHERE user_id = ? AND contact_id = ?",
            (user_id, contact_id),
        )
        existing = cur.fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="contact not found")

        cur.execute(
            """
            SELECT snapshot
            FROM contact_history
            WHERE history_id = ? AND user_id = ? AND contact_id = ?
            """,
            (body.history_id, user_id, contact_id),
        )
        history = cur.fetchone()
        if history is None:
            raise HTTPException(status_code=404, detail="history not found")

        snapshot = json.loads(history["snapshot"])
        write_contact_history(cur, existing, user_id, now)
        cur.execute(
            """
            UPDATE contacts
            SET display_name = ?, given_name = ?, family_name = ?,
                phone_numbers = ?, email_addresses = ?, postal_addresses = ?,
                organization = ?, job_title = ?, notes = ?, photo_uri = ?,
                source_device_id = ?, version = ?, sync_status = ?, hash = ?,
                updated_at = ?, deleted_at = ?
            WHERE contact_id = ?
            """,
            (
                snapshot.get("display_name", ""),
                snapshot.get("given_name", ""),
                snapshot.get("family_name", ""),
                json.dumps(snapshot.get("phone_numbers", []), ensure_ascii=False),
                json.dumps(snapshot.get("email_addresses", []), ensure_ascii=False),
                json.dumps(snapshot.get("postal_addresses", []), ensure_ascii=False),
                snapshot.get("organization", ""),
                snapshot.get("job_title", ""),
                snapshot.get("notes", ""),
                snapshot.get("photo_uri", ""),
                body.device_id,
                int(existing["version"]) + 1,
                "synced",
                snapshot.get("hash", existing["hash"]),
                now,
                snapshot.get("deleted_at"),
                contact_id,
            ),
        )
        write_audit(
            cur,
            "contact.rollback",
            user_id=user_id,
            target_type="contact",
            target_id=contact_id,
            metadata={"history_id": body.history_id},
        )
        conn.commit()
        cache_delete(f"cnt:{user_id}")
    finally:
        conn.close()
    return {"status": "rolled_back", "contact_id": contact_id, "history_id": body.history_id}


@app.get("/api/v1/dedupe/candidates")
def dedupe_candidates(
    min_score: int = Query(default=75, ge=1, le=100),
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM contacts
            WHERE user_id = ? AND deleted_at IS NULL
            ORDER BY updated_at DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
        cur.execute("SELECT pair_key FROM dedupe_ignore WHERE user_id = ?", (user_id,))
        ignored = {row["pair_key"] for row in cur.fetchall()}
    finally:
        conn.close()

    contacts = [serialize_contact(row) for row in rows]

    # Build inverted indexes for blocking (avoids O(n^2) full comparison)
    phone_index: Dict[str, set] = defaultdict(set)
    email_index: Dict[str, set] = defaultdict(set)
    for i, c in enumerate(contacts):
        for item in c.get("phone_numbers", []):
            norm = normalize_phone(str(item.get("value", "")))
            if norm:
                phone_index[norm].add(i)
        for item in c.get("email_addresses", []):
            raw = str(item.get("value", "")).strip().lower()
            if raw:
                email_index[raw].add(i)

    # Collect candidate pairs: only those sharing at least one phone or email
    candidate_pairs: set = set()
    for indices in phone_index.values():
        idx_list = sorted(indices)
        for a in range(len(idx_list)):
            for b in range(a + 1, len(idx_list)):
                candidate_pairs.add((idx_list[a], idx_list[b]))
    for indices in email_index.values():
        idx_list = sorted(indices)
        for a in range(len(idx_list)):
            for b in range(a + 1, len(idx_list)):
                candidate_pairs.add((idx_list[a], idx_list[b]))

    # For very low thresholds, name-only matches could qualify (max without
    # phone/email = 0.2*100 + 0.1*100 = 30), so fall back to full comparison.
    if min_score <= 30:
        for i in range(len(contacts)):
            for j in range(i + 1, len(contacts)):
                candidate_pairs.add((i, j))

    candidates = []
    for i, j in candidate_pairs:
        left, right = contacts[i], contacts[j]
        key = pair_key(left["local_id"], right["local_id"])
        if key in ignored:
            continue
        score = dedupe_score(left, right)
        if score["total"] < min_score:
            continue
        confidence = "high" if score["total"] >= 90 else "suspected"
        candidates.append(
            {
                "pair_key": key,
                "confidence": confidence,
                "score": score,
                "left": left,
                "right": right,
            }
        )

    candidates.sort(key=lambda x: x["score"]["total"], reverse=True)
    return {"count": len(candidates), "items": candidates}


@app.post("/api/v1/dedupe/ignore")
def dedupe_ignore(body: DedupeIgnoreRequest, user_id: str = Depends(get_current_user_id)) -> Dict[str, str]:
    key = pair_key(body.local_id_a, body.local_id_b)
    created_at = now_iso()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT OR IGNORE INTO dedupe_ignore (ignore_id, user_id, pair_key, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (str(uuid.uuid4()), user_id, key, created_at),
        )
        write_audit(
            cur,
            "dedupe.ignore",
            user_id=user_id,
            target_type="dedupe_pair",
            target_id=key,
        )
        conn.commit()
    finally:
        conn.close()
    return {"status": "ignored", "pair_key": key}


@app.post("/api/v1/dedupe/merge")
def dedupe_merge(body: DedupeMergeRequest, user_id: str = Depends(get_current_user_id)) -> Dict[str, Any]:
    unique_ids = list(dict.fromkeys(body.local_ids))
    now = now_iso()
    if len(unique_ids) < 2:
        raise HTTPException(status_code=400, detail="need at least 2 unique local_ids")

    conn = get_conn()
    try:
        cur = conn.cursor()
        q_marks = ",".join(["?"] * len(unique_ids))
        cur.execute(
            f"SELECT * FROM contacts WHERE user_id = ? AND deleted_at IS NULL AND local_id IN ({q_marks})",
            [user_id] + unique_ids,
        )
        rows = cur.fetchall()
        if len(rows) != len(unique_ids):
            raise HTTPException(status_code=404, detail="some contacts not found")

        contacts = [serialize_contact(row) for row in rows]
        by_local_id = {c["local_id"]: c for c in contacts}
        rows_by_local_id = {row["local_id"]: row for row in rows}

        # pick primary as the one with the most complete name+fields
        def completeness(c: Dict[str, Any]) -> int:
            return (
                len(c.get("display_name", "") or "")
                + len(c.get("given_name", "") or "")
                + len(c.get("family_name", "") or "")
                + 5 * len(c.get("phone_numbers", []))
                + 3 * len(c.get("email_addresses", []))
            )

        primary_local_id = sorted(unique_ids, key=lambda x: completeness(by_local_id[x]), reverse=True)[0]
        merged = dict(by_local_id[primary_local_id])
        for local_id in unique_ids:
            if local_id == primary_local_id:
                continue
            merged = merge_two_contacts(merged, by_local_id[local_id])

        # Persist merge: update primary, soft-delete others.
        primary_row = rows_by_local_id[primary_local_id]
        write_contact_history(cur, primary_row, user_id, now)
        merged_change = SyncChange(
            op="upsert",
            local_id=primary_local_id,
            display_name=merged.get("display_name", ""),
            given_name=merged.get("given_name", ""),
            family_name=merged.get("family_name", ""),
            phone_numbers=merged.get("phone_numbers", []),
            email_addresses=merged.get("email_addresses", []),
            postal_addresses=merged.get("postal_addresses", []),
            organization=merged.get("organization", ""),
            job_title=merged.get("job_title", ""),
            notes=merged.get("notes", ""),
            source_device_id=body.device_id,
        )
        merged_hash = sync_change_fingerprint(merged_change)
        cur.execute(
            """
            UPDATE contacts
            SET display_name = ?, given_name = ?, family_name = ?,
                phone_numbers = ?, email_addresses = ?, postal_addresses = ?,
                organization = ?, job_title = ?, notes = ?, photo_uri = ?,
                source_device_id = ?, version = ?, sync_status = ?, hash = ?,
                updated_at = ?, deleted_at = NULL
            WHERE contact_id = ?
            """,
            (
                merged_change.display_name,
                merged_change.given_name,
                merged_change.family_name,
                json.dumps(merged_change.phone_numbers, ensure_ascii=False),
                json.dumps(merged_change.email_addresses, ensure_ascii=False),
                json.dumps(merged_change.postal_addresses, ensure_ascii=False),
                merged_change.organization,
                merged_change.job_title,
                merged_change.notes,
                merged.get("photo_uri", ""),
                body.device_id,
                int(primary_row["version"]) + 1,
                "synced",
                merged_hash,
                now,
                primary_row["contact_id"],
            ),
        )

        deleted_count = 0
        for local_id in unique_ids:
            if local_id == primary_local_id:
                continue
            row = rows_by_local_id[local_id]
            write_contact_history(cur, row, user_id, now)
            cur.execute(
                """
                UPDATE contacts
                SET deleted_at = ?, updated_at = ?, source_device_id = ?, sync_status = ?
                WHERE contact_id = ?
                """,
                (now, now, body.device_id, "synced", row["contact_id"]),
            )
            deleted_count += 1

        write_audit(
            cur,
            "dedupe.merge",
            user_id=user_id,
            target_type="contact",
            target_id=primary_local_id,
            metadata={"merged_count": deleted_count + 1},
        )
        conn.commit()
        cache_delete(f"cnt:{user_id}")
    finally:
        conn.close()

    return {
        "status": "merged",
        "primary_local_id": primary_local_id,
        "merged_count": deleted_count + 1,
        "deleted_count": deleted_count,
    }


def vcard_escape(value: str) -> str:
    text = (value or "").replace("\\", "\\\\").replace("\n", "\\n")
    return re.sub(r"[;,]", lambda m: "\\" + m.group(0), text)


@app.get("/api/v1/contacts/export.vcf", response_class=PlainTextResponse)
def export_contacts_vcf(user_id: str = Depends(get_current_user_id)) -> PlainTextResponse:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM contacts WHERE user_id = ? AND deleted_at IS NULL ORDER BY updated_at DESC",
            (user_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    cards = []
    for row in rows:
        phones = json.loads(row["phone_numbers"] or "[]")
        emails = json.loads(row["email_addresses"] or "[]")
        lines = [
            "BEGIN:VCARD",
            "VERSION:3.0",
            "FN:" + vcard_escape(row["display_name"] or ""),
            "N:{};{};;;".format(vcard_escape(row["family_name"] or ""), vcard_escape(row["given_name"] or "")),
        ]
        for p in phones:
            ptype = (p.get("type") or "CELL").upper()
            lines.append("TEL;TYPE={}:{}".format(vcard_escape(ptype), vcard_escape(str(p.get("value", "")))))
        for e in emails:
            etype = (e.get("type") or "INTERNET").upper()
            lines.append("EMAIL;TYPE={}:{}".format(vcard_escape(etype), vcard_escape(str(e.get("value", "")))))
        if row["organization"]:
            lines.append("ORG:" + vcard_escape(row["organization"]))
        if row["job_title"]:
            lines.append("TITLE:" + vcard_escape(row["job_title"]))
        if row["notes"]:
            lines.append("NOTE:" + vcard_escape(row["notes"]))
        lines.append("END:VCARD")
        cards.append("\r\n".join(lines))

    content = "\r\n".join(cards) + ("\r\n" if cards else "")
    return PlainTextResponse(
        content=content,
        headers={"Content-Disposition": 'attachment; filename="contactsync-export.vcf"'},
    )


@app.get("/api/v1/metrics")
def app_metrics() -> Dict[str, Any]:
    items = []
    total_requests = 0
    total_errors = 0
    for key, value in METRICS.items():
        count = int(value["count"])
        errors = int(value["error_count"])
        avg_ms = (value["total_ms"] / value["count"]) if value["count"] else 0.0
        items.append({"endpoint": key, "count": count, "error_count": errors, "avg_latency_ms": round(avg_ms, 2)})
        total_requests += count
        total_errors += errors
    items.sort(key=lambda x: x["endpoint"])
    return {"total_requests": total_requests, "total_errors": total_errors, "items": items}


@app.get("/api/v1/audit")
def list_audit_logs(
    limit: int = Query(default=50, ge=1, le=200),
    user_id: str = Depends(get_current_user_id),
) -> Dict[str, Any]:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT audit_id, action, target_type, target_id, metadata, created_at
            FROM audit_log
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    items = []
    for row in rows:
        items.append(
            {
                "audit_id": row["audit_id"],
                "action": row["action"],
                "target_type": row["target_type"],
                "target_id": row["target_id"],
                "metadata": json.loads(row["metadata"] or "{}"),
                "created_at": row["created_at"],
            }
        )
    return {"count": len(items), "items": items}


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
            "SELECT * FROM contacts WHERE user_id = ? AND contact_id = ? AND deleted_at IS NULL",
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

        write_contact_history(cur, row, user_id, now_iso())
        cur.execute(
            "UPDATE contacts SET photo_uri = ?, updated_at = ? WHERE contact_id = ?",
            (uri, now_iso(), contact_id),
        )
        conn.commit()
    finally:
        conn.close()

    return {"photo_uri": uri}
