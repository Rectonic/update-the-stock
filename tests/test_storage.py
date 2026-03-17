import tempfile
import unittest
from pathlib import Path

from app.config import Settings
from app.storage import Storage


class StorageTests(unittest.TestCase):
    def test_fail_incomplete_runs_marks_running_and_queued_as_failed(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp) / "data"
            settings = Settings(
                data_dir=data_dir,
                wc_base_url="https://example.com",
                wc_consumer_key="ck_test",
                wc_consumer_secret="cs_test",
                request_timeout_seconds=30,
                app_auth_username="admin",
                app_auth_password="secret",
            )
            storage = Storage(settings)
            storage.ensure_dirs()

            queued = storage.create_run("queued-run", "upload-1")
            running = storage.create_run("running-run", "upload-2")
            completed = storage.create_run("completed-run", "upload-3")

            storage.save_run({**running, "status": "running", "started_at": "2026-03-17T00:00:00+00:00"})
            storage.save_run(
                {
                    **completed,
                    "status": "completed",
                    "started_at": "2026-03-17T00:00:00+00:00",
                    "finished_at": "2026-03-17T00:01:00+00:00",
                }
            )

            updated = storage.fail_incomplete_runs("Worker stopped")

            self.assertEqual(updated, 2)

            queued_after = storage.load_run("queued-run")
            running_after = storage.load_run("running-run")
            completed_after = storage.load_run("completed-run")

            self.assertEqual(queued_after["status"], "failed")
            self.assertEqual(running_after["status"], "failed")
            self.assertEqual(completed_after["status"], "completed")
            self.assertIn("Worker stopped", queued_after["errors"])
            self.assertIn("Worker stopped", running_after["errors"])


if __name__ == "__main__":
    unittest.main()
