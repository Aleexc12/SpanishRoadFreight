"""
Extract quarterly price index from EPTMC sheet 2.9
Sources: eptmc2020 for 2019-2020 quarters, eptmc2024 for 2021-2024 quarters
Output: price_index.csv
"""

import xlrd
import csv

DISTANCE_BANDS = [
    "Total",
    "50 km y menos",
    "Entre 51 y 100 km",
    "Entre 101 y 200 km",
    "Entre 201 y 300 km",
    "Mas de 300 km",
]

QUARTER_MAP = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}


def extract_quarterly(filepath, year_min, year_max):
    """Extract quarterly rows from sheet 2.9 between year_min and year_max inclusive."""
    wb = xlrd.open_workbook(filepath)
    sh = wb.sheet_by_name("2.9")

    rows = []
    current_year = None

    for r in range(23, sh.nrows):  # quarterly section starts at row 23
        col0 = sh.cell_value(r, 0)
        col1 = sh.cell_value(r, 1)

        # Year header row: year in col0, rest empty
        if col0 != "":
            try:
                current_year = int(float(col0))
            except (ValueError, TypeError):
                if isinstance(col0, str) and col0.strip().isdigit():
                    current_year = int(col0.strip())
                else:
                    continue

        # Quarter data row: quarter label in col1
        elif isinstance(col1, str) and col1.strip() in QUARTER_MAP:
            if current_year is None:
                continue
            if current_year < year_min or current_year > year_max:
                continue

            quarter = QUARTER_MAP[col1.strip()]
            values = [sh.cell_value(r, c) for c in range(2, 8)]

            for i, band in enumerate(DISTANCE_BANDS):
                rows.append({
                    "year": current_year,
                    "quarter": quarter,
                    "distance_band": band,
                    "price_index": values[i],
                })

    return rows


# Extract from two files
rows_2019_2020 = extract_quarterly("EPTMC/eptmc2020.tablas.xls", 2019, 2020)
rows_2021_2024 = extract_quarterly("EPTMC/eptmc2024.tablas.xls", 2021, 2024)

all_rows = rows_2019_2020 + rows_2021_2024
all_rows.sort(key=lambda r: (r["year"], r["quarter"], r["distance_band"]))

# Write CSV
output_path = "HW6/price_index.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["year", "quarter", "distance_band", "price_index"])
    writer.writeheader()
    writer.writerows(all_rows)

print(f"Wrote {len(all_rows)} rows to {output_path}")
print(f"Years: {sorted(set(r['year'] for r in all_rows))}")
print(f"Quarters per year: {len(all_rows) // len(set(r['year'] for r in all_rows)) if all_rows else 0}")
print("\nFirst 10 rows:")
for r in all_rows[:10]:
    print(f"  {r}")
print("\nLast 5 rows:")
for r in all_rows[-5:]:
    print(f"  {r}")
