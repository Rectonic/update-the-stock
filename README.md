# WooCommerce Stock/Price Sync UI

FastAPI dashboard to upload official `.xlsx` files, parse SKU/stock/price, and run WooCommerce sync jobs.

## What it does

- Upload official Excel (`A=SKU`, `C=Quantity`, `D=Price`) with mixed header/data first row support.
- Normalize SKU by removing whitespace.
- Normalize price like `72 000,00 -> 72000` (fraction removed).
- Aggregate duplicate SKUs by summed quantity.
- Update WooCommerce simple + variation products by SKU.
- For Woo SKUs absent in upload: set stock to `0` (price unchanged).
- Save run audit CSV under `data/audits/`.

## Environment

Copy `.env.example` to `.env` and set:

- `WC_BASE_URL`
- `WC_CONSUMER_KEY`
- `WC_CONSUMER_SECRET`
- `APP_AUTH_USERNAME`
- `APP_AUTH_PASSWORD`

Backward-compatible env names `user_key` and `secret_key` are also supported.

## Local run

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open `http://localhost:8000`.
The browser will prompt for HTTP Basic credentials.

## Docker

```bash
docker compose up --build
```

Data persists in `./data`.

## API endpoints

- `POST /upload`
- `POST /runs/{upload_id}/start`
- `GET /runs/{run_id}` (add `?format=json` for JSON)
- `GET /runs/{run_id}/audit.csv`
