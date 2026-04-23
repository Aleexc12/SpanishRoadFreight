"""
Extract distance band data from EPTMC sheets 4.9-4.12.
Each sheet has year rows x 4 distance bands, split by service type.

Sheets:
  4.9  = operations_total (loaded + empty)
  4.10 = operations_loaded
  4.11 = tonnes_thousands
  4.12 = tonne_km_millions
"""

import xlrd
import csv

FILES = {
    2019: "EPTMC/eptmc2019tablas.xls",
    2020: "EPTMC/eptmc2020.tablas.xls",
    2021: "EPTMC/eptmc2021.tablas.xls",
    2022: "EPTMC/eptmc2022.tablas.xls",
    2023: "EPTMC/eptmc2023.tablas.xls",
    2024: "EPTMC/eptmc2024.tablas.xls",
}

METRICS = {
    "operations_total":  "4.9",
    "operations_loaded": "4.10",
    "tonnes_thousands":  "4.11",
    "tonne_km_millions": "4.12",
}

DISTANCE_BANDS = ["Total", "Menos de 50 km", "De 50 a 149 km", "De 150 a 500 km", "Mas de 500 km"]
BAND_COLS = [1, 2, 3, 4, 5]  # Total, <50, 50-149, 150-500, >500

# Row ranges for each service type section (same across all sheets and years)
SECTIONS = {
    "total":              (15, 22),  # rows 15-21
    "cuenta_ajena":       (25, 32),  # rows 25-31
    "cuenta_propia":      (35, 42),  # rows 35-41
}

# We only need years 2018-2024, and only from the 2024 file (it has all years)
# But to be safe and match the approach of other extractions, we'll read from each file
# Actually, since each file has 7 years of data and we'd get duplicates,
# let's just use the 2024 file which has 2018-2024 all at once.


def extract_from_file(filepath):
    wb = xlrd.open_workbook(filepath)
    rows = []

    for metric, sheet_name in METRICS.items():
        sh = wb.sheet_by_name(sheet_name)

        for service_type, (row_start, row_end) in SECTIONS.items():
            for r in range(row_start, row_end):
                year_val = sh.cell_value(r, 0)
                if not isinstance(year_val, (int, float)):
                    continue
                year = int(year_val)
                if year < 2018 or year > 2024:
                    continue

                for band, col in zip(DISTANCE_BANDS, BAND_COLS):
                    val = sh.cell_value(r, col)
                    if isinstance(val, (int, float)):
                        rows.append({
                            "year": year,
                            "distance_band": band,
                            "service_type": service_type,
                            "metric": metric,
                            "value": val,
                        })

    return rows


# Use 2024 file, it has 2018-2024 for all sections
print("Extracting from eptmc2024...")
all_rows = extract_from_file("EPTMC/eptmc2024.tablas.xls")
all_rows.sort(key=lambda r: (r["year"], r["metric"], r["service_type"], r["distance_band"]))

output_path = "HW6/distance_bands.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["year", "distance_band", "service_type", "metric", "value"])
    writer.writeheader()
    writer.writerows(all_rows)

print(f"\nWrote {len(all_rows)} rows to {output_path}")
print(f"Years: {sorted(set(r['year'] for r in all_rows))}")
print(f"Metrics: {sorted(set(r['metric'] for r in all_rows))}")
print(f"Service types: {sorted(set(r['service_type'] for r in all_rows))}")
print(f"Distance bands: {sorted(set(r['distance_band'] for r in all_rows))}")

# Quick distribution stability check (Technical.md requirement)
print("\n--- Distribution stability check (total ops, share by band) ---")
for year in range(2018, 2025):
    total_row = [r for r in all_rows if r["year"] == year and r["metric"] == "operations_total"
                 and r["service_type"] == "total" and r["distance_band"] == "Total"]
    band_rows = [r for r in all_rows if r["year"] == year and r["metric"] == "operations_total"
                 and r["service_type"] == "total" and r["distance_band"] != "Total"]
    if total_row:
        total_val = total_row[0]["value"]
        shares = {r["distance_band"]: r["value"] / total_val * 100 for r in band_rows}
        print(f"  {year}: " + "  ".join(f"{b}: {s:.1f}%" for b, s in sorted(shares.items())))
