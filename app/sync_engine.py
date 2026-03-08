from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional

from app.woo_client import ProductRef, WooCommerceClient, WooAPIError


@dataclass
class SyncReport:
    metrics: Dict[str, int]
    errors: List[str]
    audit_rows: List[Dict[str, str]]


def _to_stock_quantity(quantity: float) -> int:
    if quantity <= 0:
        return 0
    return int(quantity)


def sync_to_woocommerce(
    snapshot: Dict[str, object],
    client: WooCommerceClient,
    logger: Optional[Callable[[str], None]] = None,
) -> SyncReport:
    items = snapshot.get("items", {})
    if not isinstance(items, dict):
        raise ValueError("Invalid snapshot format: items must be a dictionary")

    def log(message: str) -> None:
        if logger:
            logger(message)

    by_sku, duplicate_skus = client.fetch_catalog()
    log(f"Fetched WooCommerce catalog with {len(by_sku)} unique SKUs")

    errors: List[str] = []
    audit_rows: List[Dict[str, str]] = []

    metrics = {
        "parsed_rows": int(snapshot.get("stats", {}).get("data_rows", 0)),
        "duplicate_sku_keys": int(snapshot.get("stats", {}).get("duplicate_sku_keys", 0)),
        "catalog_skus": len(by_sku),
        "catalog_duplicate_skus": len(duplicate_skus),
        "matched_products": 0,
        "updated_stock_count": 0,
        "updated_price_count": 0,
        "absent_set_to_zero_count": 0,
        "missing_in_wp_count": 0,
        "failures": 0,
    }

    if duplicate_skus:
        for sku in duplicate_skus:
            errors.append(f"Duplicate SKU in WooCommerce catalog: {sku}")

    normalized_input: Dict[str, Dict[str, object]] = {}
    for sku, payload in items.items():
        if not isinstance(payload, dict):
            continue
        normalized_input[str(sku)] = payload

    for sku, payload in normalized_input.items():
        quantity = float(payload.get("quantity", 0.0))
        stock_quantity = _to_stock_quantity(quantity)
        price = payload.get("price")
        price_str = str(price) if price is not None else None

        ref = by_sku.get(sku)
        if ref is None:
            metrics["missing_in_wp_count"] += 1
            audit_rows.append(
                {
                    "sku": sku,
                    "quantity": str(stock_quantity),
                    "availability": "1" if stock_quantity > 0 else "0",
                    "price": price_str or "",
                    "status": "missing_in_wp",
                    "message": "SKU not found in WooCommerce",
                    "wp_type": "",
                    "wp_id": "",
                    "wp_parent_id": "",
                }
            )
            continue

        metrics["matched_products"] += 1
        try:
            client.update_item(ref, stock_quantity=stock_quantity, regular_price=price_str)
            metrics["updated_stock_count"] += 1
            if price_str is not None:
                metrics["updated_price_count"] += 1

            audit_rows.append(
                {
                    "sku": sku,
                    "quantity": str(stock_quantity),
                    "availability": "1" if stock_quantity > 0 else "0",
                    "price": price_str or "",
                    "status": "updated" if price_str is not None else "price_kept",
                    "message": "",
                    "wp_type": ref.kind,
                    "wp_id": str(ref.product_id),
                    "wp_parent_id": str(ref.parent_id or ""),
                }
            )
        except WooAPIError as exc:
            metrics["failures"] += 1
            err = f"Failed to update SKU {sku}: {exc}"
            errors.append(err)
            audit_rows.append(
                {
                    "sku": sku,
                    "quantity": str(stock_quantity),
                    "availability": "1" if stock_quantity > 0 else "0",
                    "price": price_str or "",
                    "status": "error",
                    "message": str(exc),
                    "wp_type": ref.kind,
                    "wp_id": str(ref.product_id),
                    "wp_parent_id": str(ref.parent_id or ""),
                }
            )

    for sku, ref in by_sku.items():
        if sku in normalized_input:
            continue

        try:
            client.update_item(ref, stock_quantity=0, regular_price=None)
            metrics["absent_set_to_zero_count"] += 1
            audit_rows.append(
                {
                    "sku": sku,
                    "quantity": "0",
                    "availability": "0",
                    "price": "",
                    "status": "absent_set_to_zero",
                    "message": "SKU absent in uploaded file",
                    "wp_type": ref.kind,
                    "wp_id": str(ref.product_id),
                    "wp_parent_id": str(ref.parent_id or ""),
                }
            )
        except WooAPIError as exc:
            metrics["failures"] += 1
            err = f"Failed to set SKU {sku} stock to 0: {exc}"
            errors.append(err)
            audit_rows.append(
                {
                    "sku": sku,
                    "quantity": "0",
                    "availability": "0",
                    "price": "",
                    "status": "error",
                    "message": str(exc),
                    "wp_type": ref.kind,
                    "wp_id": str(ref.product_id),
                    "wp_parent_id": str(ref.parent_id or ""),
                }
            )

    return SyncReport(metrics=metrics, errors=errors, audit_rows=audit_rows)


def generate_audit_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "sku",
        "quantity",
        "availability",
        "price",
        "status",
        "message",
        "wp_type",
        "wp_id",
        "wp_parent_id",
    ]

    with path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
