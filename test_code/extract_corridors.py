"""
Extract O/D corridor data from EPTMC sheets 6.5-6.8 (+ s supplements).
Each year's file has one matrix per metric. This script reads all 6 years
and outputs a single long-format CSV.

Sheets:
  6.5 / 6.5s = transport_operations_total
  6.6 / 6.6s = transport_operations_empty
  6.7 / 6.7s = tonnes_thousands
  6.8 / 6.8s = tonne_km_millions
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
    "transport_operations_total": ("6.5", "6.5s"),
    "transport_operations_empty": ("6.6", "6.6s"),
    "tonnes_thousands":          ("6.7", "6.7s"),
    "tonne_km_millions":         ("6.8", "6.8s"),
}

# Canonical region names (from row labels in the data)
REGIONS = [
    "Andalucia", "Aragon", "Asturias", "Baleares", "Canarias",
    "Cantabria", "Castilla-La Mancha", "Castilla y Leon", "Cataluna",
    "Com. Valenciana", "Extremadura", "Galicia", "Madrid",
    "Murcia", "Navarra", "Pais Vasco", "Rioja", "Ceuta y Melilla",
]

# Regions to exclude (islands + enclaves, suppressed in interregional cells)
EXCLUDED_REGIONS = {"Baleares", "Canarias", "Ceuta y Melilla"}

DATA_ROW_START = 17  # first origin row in all sheets
DATA_ROW_END = 35    # exclusive (18 regions: rows 17-34)

# Left sheet: cols 1-10 = first 10 destinations
# Right sheet (s): cols 0-6 = remaining 7 destinations, col 7 = Ceuta, col 8 = TOTAL, col 9 = "del cual"
LEFT_DEST_COLS = list(range(1, 11))   # 10 destinations
RIGHT_DEST_COLS = list(range(0, 8))   # 8 destinations (including Ceuta y Melilla)


def clean_value(val):
    """Convert cell value to float or None for suppressed/missing."""
    if isinstance(val, (int, float)):
        return val
    if isinstance(val, str):
        stripped = val.strip()
        if stripped in ("", "..", ".", "-"):
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def extract_matrix(wb, sheet_left, sheet_right):
    """Extract 18x18 O/D matrix from a pair of sheets. Returns dict of (row_idx, col_idx) -> value."""
    sh_l = wb.sheet_by_name(sheet_left)
    sh_r = wb.sheet_by_name(sheet_right)

    matrix = {}
    for r_idx, r in enumerate(range(DATA_ROW_START, DATA_ROW_END)):
        # Left sheet: 10 destinations
        for c_idx, c in enumerate(LEFT_DEST_COLS):
            val = clean_value(sh_l.cell_value(r, c))
            matrix[(r_idx, c_idx)] = val

        # Right sheet: 8 destinations (cols 0-7)
        for c_idx_offset, c in enumerate(RIGHT_DEST_COLS):
            val = clean_value(sh_r.cell_value(r, c))
            matrix[(r_idx, 10 + c_idx_offset)] = val

    return matrix


def extract_all():
    all_rows = []

    for year, filepath in FILES.items():
        print(f"Processing {filepath} ({year})...")
        wb = xlrd.open_workbook(filepath)

        for metric, (sheet_l, sheet_r) in METRICS.items():
            matrix = extract_matrix(wb, sheet_l, sheet_r)

            for orig_idx, origin in enumerate(REGIONS):
                for dest_idx, destination in enumerate(REGIONS):
                    # Skip diagonal (intra-community)
                    if orig_idx == dest_idx:
                        continue
                    # Skip excluded regions
                    if origin in EXCLUDED_REGIONS or destination in EXCLUDED_REGIONS:
                        continue

                    val = matrix.get((orig_idx, dest_idx))
                    if val is None:
                        continue

                    all_rows.append({
                        "year": year,
                        "origin_region": origin,
                        "destination_region": destination,
                        "metric": metric,
                        "value": val,
                    })

    return all_rows


rows = extract_all()
rows.sort(key=lambda r: (r["year"], r["metric"], r["origin_region"], r["destination_region"]))

output_path = "HW6/corridors.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["year", "origin_region", "destination_region", "metric", "value"])
    writer.writeheader()
    writer.writerows(rows)

# Summary
years = sorted(set(r["year"] for r in rows))
metrics = sorted(set(r["metric"] for r in rows))
origins = sorted(set(r["origin_region"] for r in rows))

print(f"\nWrote {len(rows)} rows to {output_path}")
print(f"Years: {years}")
print(f"Metrics: {metrics}")
print(f"Regions (origins): {len(origins)}: {origins}")
print(f"Rows per year per metric: {len(rows) // len(years) // len(metrics)}")

# Spot check: Andalucia -> Cataluna total ops 2024
spot = [r for r in rows if r["year"] == 2024 and r["origin_region"] == "Andalucia"
        and r["destination_region"] == "Cataluna" and r["metric"] == "transport_operations_total"]
if spot:
    print(f"\nSpot check: Andalucia->Cataluna total ops 2024 = {spot[0]['value']}")
