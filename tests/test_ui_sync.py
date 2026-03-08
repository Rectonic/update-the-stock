import csv
import tempfile
import unittest
from pathlib import Path

import openpyxl

from app.parser import normalize_price, parse_official_xlsx
from app.sync_engine import generate_audit_csv, sync_to_woocommerce
from app.woo_client import ProductRef, WooAPIError


class FakeWooClient:
    def __init__(self, by_sku, duplicates=None, fail_skus=None):
        self.by_sku = by_sku
        self.duplicates = duplicates or []
        self.fail_skus = set(fail_skus or [])
        self.calls = []

    def fetch_catalog(self):
        return self.by_sku, self.duplicates

    def update_item(self, ref, stock_quantity, regular_price):
        if ref.sku in self.fail_skus:
            raise WooAPIError(f"forced failure for {ref.sku}")
        self.calls.append((ref.sku, stock_quantity, regular_price, ref.kind))


class ParserAndSyncTests(unittest.TestCase):
    def test_normalize_price_localized_and_decimal(self):
        self.assertEqual(normalize_price("72 000,00"), "72000")
        self.assertEqual(normalize_price("72000.99"), "72000")
        self.assertEqual(normalize_price("72 000"), "72000")
        self.assertIsNone(normalize_price(""))
        self.assertIsNone(normalize_price("abc"))

    def test_parse_with_header_and_duplicate_aggregation(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "official.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append(["Артикул", "Имя", "Количество", "Цена"])
            ws.append([" 11 11 ", "A", 3, "10 000,50"])
            ws.append(["1111", "A2", 2, None])
            ws.append(["2222", "B", 0, "20 000,00"])
            wb.save(path)

            parsed = parse_official_xlsx(path)

            self.assertEqual(parsed.stats["data_rows"], 3)
            self.assertEqual(parsed.stats["duplicate_sku_keys"], 1)
            self.assertEqual(parsed.items["1111"].quantity, 5.0)
            self.assertEqual(parsed.items["1111"].availability, 1)
            self.assertEqual(parsed.items["1111"].price, "10000")
            self.assertEqual(parsed.items["2222"].availability, 0)
            self.assertEqual(parsed.items["2222"].price, "20000")

    def test_parse_without_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "official.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append(["3333", "A", 1, "1 500,00"])
            ws.append(["4444", "B", -1, "2 000,00"])
            wb.save(path)

            parsed = parse_official_xlsx(path)

            self.assertEqual(parsed.stats["data_rows"], 2)
            self.assertEqual(parsed.items["3333"].availability, 1)
            self.assertEqual(parsed.items["4444"].availability, 0)

    def test_sync_simple_variation_missing_price_and_absent_set_to_zero(self):
        snapshot = {
            "stats": {"data_rows": 3, "duplicate_sku_keys": 1},
            "items": {
                "SIMPLE1": {"quantity": 3, "availability": 1, "price": "12000"},
                "VAR1": {"quantity": 1, "availability": 1, "price": None},
                "MISSING": {"quantity": 2, "availability": 1, "price": "5000"},
            },
        }

        catalog = {
            "SIMPLE1": ProductRef("SIMPLE1", "simple", 10, None, "10000"),
            "VAR1": ProductRef("VAR1", "variation", 22, 11, "20000"),
            "ABSENT": ProductRef("ABSENT", "simple", 30, None, "9000"),
        }
        fake = FakeWooClient(catalog)

        report = sync_to_woocommerce(snapshot=snapshot, client=fake)

        self.assertEqual(report.metrics["matched_products"], 2)
        self.assertEqual(report.metrics["missing_in_wp_count"], 1)
        self.assertEqual(report.metrics["absent_set_to_zero_count"], 1)
        self.assertEqual(report.metrics["updated_price_count"], 1)
        self.assertEqual(report.metrics["updated_stock_count"], 2)
        self.assertEqual(report.metrics["failures"], 0)

        self.assertIn(("SIMPLE1", 3, "12000", "simple"), fake.calls)
        self.assertIn(("VAR1", 1, None, "variation"), fake.calls)
        self.assertIn(("ABSENT", 0, None, "simple"), fake.calls)

    def test_sync_generates_error_rows_and_audit_csv(self):
        snapshot = {
            "stats": {"data_rows": 1, "duplicate_sku_keys": 0},
            "items": {
                "BROKEN": {"quantity": 5, "availability": 1, "price": "8000"},
            },
        }
        catalog = {
            "BROKEN": ProductRef("BROKEN", "simple", 10, None, "7000"),
        }
        fake = FakeWooClient(catalog, fail_skus={"BROKEN"})

        report = sync_to_woocommerce(snapshot=snapshot, client=fake)
        self.assertEqual(report.metrics["failures"], 1)
        self.assertTrue(any(row["status"] == "error" for row in report.audit_rows))

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "audit.csv"
            generate_audit_csv(path, report.audit_rows)
            self.assertTrue(path.exists())
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["sku"], "BROKEN")


if __name__ == "__main__":
    unittest.main()
