from __future__ import annotations

import os
import secrets
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Annotated, Dict, Optional

from fastapi import Depends, FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import load_settings
from app.parser import ParsedItem, parse_official_xlsx
from app.storage import Storage, utc_now_iso
from app.sync_engine import generate_audit_csv, sync_to_woocommerce
from app.woo_client import WooCommerceClient


def _load_local_env_file() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_local_env_file()
settings = load_settings()
storage = Storage(settings)
executor = ThreadPoolExecutor(max_workers=2)

app = FastAPI(title="WooCommerce Stock/Price Sync")
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")
security = HTTPBasic()


@app.on_event("startup")
def on_startup() -> None:
    storage.ensure_dirs()
    recovered = storage.fail_incomplete_runs(
        "Run was interrupted because the app restarted or the worker stopped."
    )
    if recovered:
        print(
            f"Recovered {recovered} incomplete run(s) and marked them as failed on startup.",
            flush=True,
        )


def require_auth(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)],
) -> None:
    username_ok = secrets.compare_digest(credentials.username, settings.app_auth_username)
    password_ok = secrets.compare_digest(credentials.password, settings.app_auth_password)
    if not (username_ok and password_ok):
        raise HTTPException(
            status_code=401,
            detail="Invalid credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


def _serialize_items_preview(items: Dict[str, ParsedItem], limit: int = 50) -> list[dict]:
    out = []
    for sku in sorted(items.keys())[:limit]:
        item = items[sku]
        out.append(
            {
                "sku": item.sku,
                "quantity": item.quantity,
                "availability": item.availability,
                "price": item.price,
            }
        )
    return out


def _snapshot_to_payload(
    upload_id: str,
    filename: str,
    source_path: Path,
    parse_result,
) -> Dict[str, object]:
    return {
        "upload_id": upload_id,
        "filename": filename,
        "source_path": str(source_path),
        "created_at": utc_now_iso(),
        "stats": parse_result.stats,
        "items": {
            sku: {
                "quantity": item.quantity,
                "availability": item.availability,
                "price": item.price,
            }
            for sku, item in parse_result.items.items()
        },
        "preview": _serialize_items_preview(parse_result.items),
    }


def _start_run_task(run_id: str, upload_id: str) -> None:
    storage.update_run(run_id, status="running", started_at=utc_now_iso())
    storage.append_log(run_id, f"Run started for upload {upload_id}")

    try:
        snapshot = storage.load_snapshot(upload_id)
        storage.append_log(run_id, "Loaded upload snapshot")

        client = WooCommerceClient(
            base_url=settings.wc_base_url,
            consumer_key=settings.wc_consumer_key,
            consumer_secret=settings.wc_consumer_secret,
            timeout_seconds=settings.request_timeout_seconds,
            logger=lambda msg: storage.append_log(run_id, msg),
        )
        storage.append_log(run_id, "WooCommerce client initialized")

        report = sync_to_woocommerce(
            snapshot=snapshot,
            client=client,
            logger=lambda msg: storage.append_log(run_id, msg),
        )

        audit_path = storage.audit_path(run_id)
        generate_audit_csv(audit_path, report.audit_rows)

        storage.update_run(
            run_id,
            status="completed",
            finished_at=utc_now_iso(),
            metrics=report.metrics,
            errors=report.errors,
            audit_csv=str(audit_path),
        )
        storage.append_log(run_id, "Run completed")
    except Exception as exc:
        storage.update_run(
            run_id,
            status="failed",
            finished_at=utc_now_iso(),
            errors=[str(exc)],
        )
        storage.append_log(run_id, f"Run failed: {exc}")


@app.get("/")
def index(request: Request, _auth: Annotated[None, Depends(require_auth)]):
    uploads = storage.list_uploads()[:20]
    runs = storage.list_runs()[:20]
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "uploads": uploads,
            "runs": runs,
        },
    )


@app.post("/upload")
async def upload_file(
    _auth: Annotated[None, Depends(require_auth)],
    file: UploadFile = File(...),
):
    filename = file.filename or "upload.xlsx"
    if not filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    upload_id = uuid.uuid4().hex
    source_path = storage.save_upload_file(upload_id, filename, content)

    parse_result = parse_official_xlsx(source_path)
    snapshot = _snapshot_to_payload(upload_id, filename, source_path, parse_result)
    storage.save_snapshot(upload_id, snapshot)

    return RedirectResponse(url=f"/uploads/{upload_id}", status_code=303)


@app.get("/uploads/{upload_id}")
def upload_detail(
    upload_id: str,
    request: Request,
    _auth: Annotated[None, Depends(require_auth)],
):
    snapshot = storage.find_upload(upload_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    related_runs = [r for r in storage.list_runs() if r.get("upload_id") == upload_id][:20]
    return templates.TemplateResponse(
        request,
        "upload_detail.html",
        {
            "upload": snapshot,
            "related_runs": related_runs,
        },
    )


@app.post("/runs/{upload_id}/start")
def start_run(upload_id: str, _auth: Annotated[None, Depends(require_auth)]):
    _ = storage.load_snapshot(upload_id)
    run_id = uuid.uuid4().hex
    storage.create_run(run_id, upload_id)

    executor.submit(_start_run_task, run_id, upload_id)
    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


@app.get("/runs/{run_id}")
def run_detail(
    run_id: str,
    request: Request,
    _auth: Annotated[None, Depends(require_auth)],
    format: Optional[str] = None,
):
    run = storage.load_run(run_id)

    wants_json = format == "json" or "application/json" in request.headers.get("accept", "")
    if wants_json:
        return JSONResponse(run)

    return templates.TemplateResponse(
        request,
        "run_detail.html",
        {
            "run": run,
        },
    )


@app.get("/history")
def history(request: Request, _auth: Annotated[None, Depends(require_auth)]):
    uploads = storage.list_uploads()
    runs = storage.list_runs()
    return templates.TemplateResponse(
        request,
        "history.html",
        {
            "uploads": uploads,
            "runs": runs,
        },
    )


@app.get("/runs/{run_id}/audit.csv")
def download_audit(run_id: str, _auth: Annotated[None, Depends(require_auth)]):
    run = storage.load_run(run_id)
    audit_csv = run.get("audit_csv")
    if not audit_csv:
        raise HTTPException(status_code=404, detail="Audit CSV is not available for this run")

    path = Path(str(audit_csv))
    if not path.exists():
        raise HTTPException(status_code=404, detail="Audit CSV file not found")

    return FileResponse(path=path, media_type="text/csv", filename=f"{run_id}.csv")
