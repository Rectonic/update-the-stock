from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    wc_base_url: str
    wc_consumer_key: str
    wc_consumer_secret: str
    request_timeout_seconds: int
    app_auth_username: str
    app_auth_password: str

    @property
    def uploads_dir(self) -> Path:
        return self.data_dir / "uploads"

    @property
    def snapshots_dir(self) -> Path:
        return self.data_dir / "snapshots"

    @property
    def runs_dir(self) -> Path:
        return self.data_dir / "runs"

    @property
    def audits_dir(self) -> Path:
        return self.data_dir / "audits"


def _read_env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip()


def load_settings() -> Settings:
    base_url = _read_env("WC_BASE_URL")
    key = _read_env("WC_CONSUMER_KEY")
    secret = _read_env("WC_CONSUMER_SECRET")

    # Backward compatibility with existing local env keys.
    if not key:
        key = _read_env("user_key")
    if not secret:
        secret = _read_env("secret_key")

    data_dir = Path(_read_env("DATA_DIR", "data")).expanduser().resolve()
    timeout_text = _read_env("WC_REQUEST_TIMEOUT_SECONDS", "30")
    try:
        timeout = int(timeout_text)
    except ValueError:
        timeout = 30

    app_user = _read_env("APP_AUTH_USERNAME", "admin")
    app_pass = _read_env("APP_AUTH_PASSWORD", "change-me")

    return Settings(
        data_dir=data_dir,
        wc_base_url=base_url,
        wc_consumer_key=key,
        wc_consumer_secret=secret,
        request_timeout_seconds=max(timeout, 1),
        app_auth_username=app_user,
        app_auth_password=app_pass,
    )
