from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import requests


class WooAPIError(Exception):
    pass


@dataclass
class ProductRef:
    sku: str
    kind: str
    product_id: int
    parent_id: Optional[int]
    regular_price: Optional[str]


class WooCommerceClient:
    def __init__(
        self,
        base_url: str,
        consumer_key: str,
        consumer_secret: str,
        timeout_seconds: int = 30,
        logger: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.timeout_seconds = timeout_seconds
        self.logger = logger

        if not self.base_url:
            raise WooAPIError("WC_BASE_URL is not configured")
        if not self.consumer_key or not self.consumer_secret:
            raise WooAPIError("WooCommerce API credentials are not configured")

    def _log(self, message: str) -> None:
        if self.logger:
            self.logger(message)

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
    ) -> Tuple[object, requests.structures.CaseInsensitiveDict]:
        url = f"{self.base_url}/wp-json/wc/v3/{path.lstrip('/')}"
        response = requests.request(
            method=method,
            url=url,
            params=params,
            json=json_body,
            auth=(self.consumer_key, self.consumer_secret),
            timeout=self.timeout_seconds,
        )

        if response.status_code >= 400:
            snippet = response.text[:300]
            raise WooAPIError(
                f"WooCommerce request failed [{response.status_code}] {method} {path}: {snippet}"
            )

        try:
            return response.json(), response.headers
        except ValueError as exc:
            raise WooAPIError(f"Non-JSON response from WooCommerce for {method} {path}") from exc

    def _paginate(self, path: str, params: Optional[dict] = None) -> List[dict]:
        base_params = dict(params or {})
        page = 1
        per_page = 100
        out: List[dict] = []
        self._log(f"Fetching {path} from WooCommerce")

        while True:
            paged_params = dict(base_params)
            paged_params.update({"per_page": per_page, "page": page})
            self._log(f"Requesting {path} page {page}")
            payload, _headers = self._request("GET", path, params=paged_params)
            if not isinstance(payload, list):
                raise WooAPIError(f"Expected list response while paging {path}")
            if not payload:
                break

            out.extend(payload)
            if len(payload) < per_page:
                break
            page += 1

        self._log(f"Finished fetching {path}: {len(out)} rows")
        return out

    def _fetch_all_simple_products(self) -> List[dict]:
        products = self._paginate("products")
        return [p for p in products if str(p.get("type")) == "simple" and p.get("sku")]

    def _fetch_all_variations(self) -> List[dict]:
        try:
            variations = self._paginate("products/variations")
            return [v for v in variations if v.get("sku")]
        except WooAPIError:
            pass

        variations: List[dict] = []
        products = self._paginate("products", params={"type": "variable"})
        for product in products:
            parent_id = product.get("id")
            if not parent_id:
                continue
            rows = self._paginate(f"products/{parent_id}/variations")
            for row in rows:
                if row.get("sku"):
                    variations.append(row)
        return variations

    def fetch_catalog(self) -> Tuple[Dict[str, ProductRef], List[str]]:
        by_sku: Dict[str, ProductRef] = {}
        duplicates: List[str] = []

        self._log("Loading simple products catalog")
        for product in self._fetch_all_simple_products():
            sku = str(product.get("sku", "")).strip()
            if not sku:
                continue
            ref = ProductRef(
                sku=sku,
                kind="simple",
                product_id=int(product["id"]),
                parent_id=None,
                regular_price=product.get("regular_price"),
            )
            if sku in by_sku:
                duplicates.append(sku)
                continue
            by_sku[sku] = ref

        self._log("Loading variations catalog")
        for variation in self._fetch_all_variations():
            sku = str(variation.get("sku", "")).strip()
            if not sku:
                continue
            ref = ProductRef(
                sku=sku,
                kind="variation",
                product_id=int(variation["id"]),
                parent_id=int(variation["parent_id"]),
                regular_price=variation.get("regular_price"),
            )
            if sku in by_sku:
                duplicates.append(sku)
                continue
            by_sku[sku] = ref

        self._log(
            f"Catalog fetch complete: {len(by_sku)} unique SKUs, {len(sorted(set(duplicates)))} duplicates"
        )
        return by_sku, sorted(set(duplicates))

    def update_item(self, ref: ProductRef, stock_quantity: int, regular_price: Optional[str]) -> None:
        payload = {
            "manage_stock": True,
            "stock_quantity": max(stock_quantity, 0),
            "stock_status": "instock" if stock_quantity > 0 else "outofstock",
        }
        if regular_price is not None:
            payload["regular_price"] = regular_price

        if ref.kind == "simple":
            self._request("PUT", f"products/{ref.product_id}", json_body=payload)
            return

        if ref.kind == "variation":
            if ref.parent_id is None:
                raise WooAPIError(f"Variation SKU {ref.sku} is missing parent_id")
            self._request(
                "PUT",
                f"products/{ref.parent_id}/variations/{ref.product_id}",
                json_body=payload,
            )
            return

        raise WooAPIError(f"Unsupported product kind for SKU {ref.sku}: {ref.kind}")
