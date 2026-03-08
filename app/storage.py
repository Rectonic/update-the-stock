from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from app.config import Settings


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class Storage:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._lock = threading.Lock()

    def ensure_dirs(self) -> None:
        for directory in (
            self.settings.data_dir,
            self.settings.uploads_dir,
            self.settings.snapshots_dir,
            self.settings.runs_dir,
            self.settings.audits_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def save_upload_file(self, upload_id: str, original_name: str, content: bytes) -> Path:
        safe_name = original_name.replace("/", "_").replace("\\", "_")
        path = self.settings.uploads_dir / f"{upload_id}__{safe_name}"
        path.write_bytes(content)
        return path

    def save_snapshot(self, upload_id: str, snapshot: Dict[str, object]) -> Path:
        path = self.settings.snapshots_dir / f"{upload_id}.json"
        payload = dict(snapshot)
        payload["upload_id"] = upload_id
        with path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        return path

    def load_snapshot(self, upload_id: str) -> Dict[str, object]:
        path = self.settings.snapshots_dir / f"{upload_id}.json"
        if not path.exists():
            raise FileNotFoundError(f"Upload snapshot not found: {upload_id}")
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def list_uploads(self) -> List[Dict[str, object]]:
        out: List[Dict[str, object]] = []
        for path in sorted(self.settings.snapshots_dir.glob("*.json"), reverse=True):
            try:
                with path.open("r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except Exception:
                continue
            out.append(data)

        out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return out

    def create_run(self, run_id: str, upload_id: str) -> Dict[str, object]:
        run = {
            "id": run_id,
            "upload_id": upload_id,
            "status": "queued",
            "created_at": utc_now_iso(),
            "started_at": None,
            "finished_at": None,
            "metrics": {},
            "errors": [],
            "logs": [],
            "audit_csv": None,
        }
        self.save_run(run)
        return run

    def run_path(self, run_id: str) -> Path:
        return self.settings.runs_dir / f"{run_id}.json"

    def save_run(self, run: Dict[str, object]) -> None:
        run_id = str(run["id"])
        path = self.run_path(run_id)
        with self._lock:
            with path.open("w", encoding="utf-8") as fh:
                json.dump(run, fh, ensure_ascii=False, indent=2, sort_keys=True)

    def load_run(self, run_id: str) -> Dict[str, object]:
        path = self.run_path(run_id)
        if not path.exists():
            raise FileNotFoundError(f"Run not found: {run_id}")
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    def update_run(self, run_id: str, **fields: object) -> Dict[str, object]:
        with self._lock:
            run = self.load_run(run_id)
            run.update(fields)
            with self.run_path(run_id).open("w", encoding="utf-8") as fh:
                json.dump(run, fh, ensure_ascii=False, indent=2, sort_keys=True)
            return run

    def append_log(self, run_id: str, message: str) -> None:
        with self._lock:
            run = self.load_run(run_id)
            logs = run.get("logs", [])
            if not isinstance(logs, list):
                logs = []
            logs.append({"ts": utc_now_iso(), "message": message})
            run["logs"] = logs
            with self.run_path(run_id).open("w", encoding="utf-8") as fh:
                json.dump(run, fh, ensure_ascii=False, indent=2, sort_keys=True)

    def list_runs(self) -> List[Dict[str, object]]:
        out: List[Dict[str, object]] = []
        for path in sorted(self.settings.runs_dir.glob("*.json"), reverse=True):
            try:
                with path.open("r", encoding="utf-8") as fh:
                    run = json.load(fh)
            except Exception:
                continue
            out.append(run)

        out.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return out

    def audit_path(self, run_id: str) -> Path:
        return self.settings.audits_dir / f"{run_id}.csv"

    def has_audit(self, run_id: str) -> bool:
        return self.audit_path(run_id).exists()

    def find_upload(self, upload_id: str) -> Optional[Dict[str, object]]:
        for upload in self.list_uploads():
            if str(upload.get("upload_id")) == upload_id:
                return upload
        return None
