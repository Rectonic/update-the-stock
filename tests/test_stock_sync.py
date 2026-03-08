import csv
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import stock_sync  # noqa: E402


class StockSyncTests(unittest.TestCase):
    def test_normalize_sku_removes_all_whitespace(self):
        self.assertEqual(stock_sync.normalize_sku(" 12 3 \t 4\n"), "1234")
        self.assertEqual(stock_sync.normalize_sku(None), "")

    def test_parse_official_workbook_sums_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            xlsx_path = Path(tmp) / "official.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append([" 100 1 ", "Name", 2])
            ws.append(["1001", "Name duplicate", 3])
            ws.append(["2002", "Name", -1])
            ws.append(["3003", "Name", 0])
            wb.save(xlsx_path)

            availability, stats = stock_sync.parse_official_workbook(xlsx_path)

            self.assertEqual(availability["1001"], 1)
            self.assertEqual(availability["2002"], 0)
            self.assertEqual(availability["3003"], 0)
            self.assertEqual(stats["official_rows"], 4)
            self.assertEqual(stats["unique_skus"], 3)
            self.assertEqual(stats["duplicate_sku_keys"], 1)

    def test_full_sync_updates_newest_csv_and_sets_missing_to_zero(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            official_dir = root / "stock_official"
            site_dir = root / "stock_site"
            official_dir.mkdir()
            site_dir.mkdir()

            xlsx_path = official_dir / "official.xlsx"
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.append(["11 11", "x", 5])
            ws.append(["2222", "x", 0])
            ws.append(["3333", "x", 1])
            ws.append(["3333", "x", -1])
            wb.save(xlsx_path)

            old_csv = site_dir / "old.csv"
            new_csv = site_dir / "new.csv"

            rows_old = [
                {"ID": "1", "Артикул": "1111", "Имя": "old", "Наличие": "0", "Базовая цена": "10"}
            ]
            rows_new = [
                {"ID": "1", "Артикул": "1111", "Имя": "A", "Наличие": "0", "Базовая цена": "10"},
                {"ID": "2", "Артикул": "2222", "Имя": "B", "Наличие": "1", "Базовая цена": "20"},
                {"ID": "3", "Артикул": "4444", "Имя": "C", "Наличие": "1", "Базовая цена": "30"},
            ]
            fieldnames = ["ID", "Артикул", "Имя", "Наличие", "Базовая цена"]

            with old_csv.open("w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows_old)

            with new_csv.open("w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows_new)

            now = time.time()
            os.utime(old_csv, (now - 200, now - 200))
            os.utime(new_csv, (now, now))

            summary = stock_sync.process_single_official_file(xlsx_path, site_dir)

            self.assertEqual(Path(summary["site_csv"]), new_csv)
            self.assertEqual(summary["matched_rows"], 2)
            self.assertEqual(summary["set_to_1"], 1)
            self.assertEqual(summary["set_to_0"], 2)

            with new_csv.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                updated = list(reader)

            self.assertEqual(updated[0]["Наличие"], "1")
            self.assertEqual(updated[1]["Наличие"], "0")
            self.assertEqual(updated[2]["Наличие"], "0")


if __name__ == "__main__":
    unittest.main()
