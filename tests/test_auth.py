import base64
import unittest

from fastapi.testclient import TestClient

from app.main import app, settings


class AuthTests(unittest.TestCase):
    def test_routes_require_basic_auth(self):
        client = TestClient(app)
        response = client.get("/")
        self.assertEqual(response.status_code, 401)

    def test_valid_basic_auth_allows_access(self):
        client = TestClient(app)
        raw = f"{settings.app_auth_username}:{settings.app_auth_password}".encode("utf-8")
        token = base64.b64encode(raw).decode("ascii")
        response = client.get("/", headers={"Authorization": f"Basic {token}"})
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
