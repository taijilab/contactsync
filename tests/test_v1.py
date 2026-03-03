import unittest
import uuid

from fastapi.testclient import TestClient

from app.main import app


class V1FlowTest(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        email = f"v1-{uuid.uuid4().hex[:8]}@example.com"
        r = self.client.post(
            "/api/v1/auth/register",
            json={"email": email, "password": "Password123"},
        )
        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.access_token = payload["access_token"]
        self.refresh_token = payload["refresh_token"]
        self.headers = {"Authorization": f"Bearer {self.access_token}"}

    def test_refresh_and_vcf_and_audit(self):
        r = self.client.post("/api/v1/auth/refresh", json={"refresh_token": self.refresh_token})
        self.assertEqual(r.status_code, 200)
        payload = r.json()
        self.assertIn("access_token", payload)
        self.assertIn("refresh_token", payload)

        # old refresh token should be invalid after rotation
        r = self.client.post("/api/v1/auth/refresh", json={"refresh_token": self.refresh_token})
        self.assertEqual(r.status_code, 401)

        new_headers = {"Authorization": f"Bearer {payload['access_token']}"}
        r = self.client.post(
            "/api/v1/contacts/batch",
            headers=new_headers,
            json={
                "contacts": [
                    {
                        "local_id": "v1c1",
                        "display_name": "Alice",
                        "given_name": "Alice",
                        "family_name": "Z",
                        "phone_numbers": [{"type": "mobile", "value": "+12025550123"}],
                        "email_addresses": [{"type": "work", "value": "alice@example.com"}],
                        "postal_addresses": [],
                    }
                ]
            },
        )
        self.assertEqual(r.status_code, 200)

        r = self.client.get("/api/v1/contacts/export.vcf", headers=new_headers)
        self.assertEqual(r.status_code, 200)
        self.assertIn("BEGIN:VCARD", r.text)
        self.assertIn("FN:Alice", r.text)

        r = self.client.get("/api/v1/audit", headers=new_headers)
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["count"], 1)

        r = self.client.get("/api/v1/metrics")
        self.assertEqual(r.status_code, 200)
        self.assertGreaterEqual(r.json()["total_requests"], 1)


if __name__ == "__main__":
    unittest.main()
