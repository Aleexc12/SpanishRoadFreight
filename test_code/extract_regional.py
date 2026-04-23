"""
Extract regional data from EPTMC sheets 6.1-6.4.
Each sheet has one row per region with columns for displacement types.

Sheets:
  6.1 = operations_total
  6.2 = operations_empty
  6.3 = tonnes_thousands
  6.4 = tonne_km_millions
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
    "operations_total":  "6.1",
    "operations_empty":  "6.2",
    "tonnes_thousands":  "6.3",
    "tonne_km_millions": "6.4",
}

# Columns layout (same across all 4 sheets):
#  0: region name
#  1: Total intrarregional
#  2: Intramunicipal
#  3: Intermunicipal intrarregional
#  4: (empty separator)
#  5: Interregional - received from other CC.AA.
#  6: Interregional - sent to other CC.AA.
#  7: (empty separator)
#  8: International - received
#  9: International - sent

DISPLACEMENT_COLS = {
    "intrarregional_total": 1,
    "intramunicipal": 2,
    "intermunicipal_intrarregional": 3,
    "interregional_recibido": 5,
    "interregional_expedido": 6,
    "internacional_recibido": 8,
    "internacional_expedido": 9,
}

# Normalize region names from the longer form used in 6.1-6.4
REGION_MAP = {
    "TOTAL": None,  # skip total row
    "Andalucía": "Andalucia",
    "Aragón": "Aragon",
    "Asturias, Principado de": "Asturias",
    "Balears, Illes": "Baleares",
    "Canarias": "Canarias",
    "Cantabria": "Cantabria",
    "Castilla-La Mancha": "Castilla-La Mancha",
    "Castilla y León": "Castilla y Leon",
    "Cataluña": "Cataluna",
    "Comunidad Valenciana": "Com. Valenciana",
    "Extremadura": "Extremadura",
    "Galicia": "Galicia",
    "Madrid, Comunidad de": "Madrid",
    "Murcia, Región de": "Murcia",
    "Navarra, C. Foral de": "Navarra",
    "País Vasco": "Pais Vasco",
    "Rioja, La": "Rioja",
    "Ceuta y Melilla": "Ceuta y Melilla",
}

DATA_ROW_START = 17
DATA_ROW_END = 36  # rows 17 (TOTAL) through 35 (Ceuta)


def clean_value(val):
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


def normalize_region(raw_name):
    """Try to match raw region name to canonical name."""
    raw = raw_name.strip()
    if raw in REGION_MAP:
        return REGION_MAP[raw]
    # Encoding issues, try matching without accents
    for key, canon in REGION_MAP.items():
        # Simple check: if the raw name starts with the same chars
        if raw[:6] == key[:6]:
            return canon
    return raw  # fallback: return as-is


def extract_regional(filepath, year):
    wb = xlrd.open_workbook(filepath)
    rows = []

    for metric, sheet_name in METRICS.items():
        sh = wb.sheet_by_name(sheet_name)

        for r in range(DATA_ROW_START, min(DATA_ROW_END, sh.nrows)):
            raw_region = sh.cell_value(r, 0)
            if not isinstance(raw_region, str) or raw_region.strip() == "":
                continue

            region = normalize_region(raw_region)
            if region is None:  # TOTAL row
                continue

            for disp_type, col in DISPLACEMENT_COLS.items():
                val = clean_value(sh.cell_value(r, col))
                if val is None:
                    continue

                rows.append({
                    "year": year,
                    "region": region,
                    "displacement_type": disp_type,
                    "metric": metric,
                    "value": val,
                })

    return rows


all_rows = []
for year, filepath in FILES.items():
    print(f"Processing {filepath} ({year})...")
    all_rows.extend(extract_regional(filepath, year))

all_rows.sort(key=lambda r: (r["year"], r["metric"], r["region"], r["displacement_type"]))

output_path = "HW6/regional.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["year", "region", "displacement_type", "metric", "value"])
    writer.writeheader()
    writer.writerows(all_rows)

regions = sorted(set(r["region"] for r in all_rows))
metrics = sorted(set(r["metric"] for r in all_rows))
disps = sorted(set(r["displacement_type"] for r in all_rows))

print(f"\nWrote {len(all_rows)} rows to {output_path}")
print(f"Years: {sorted(set(r['year'] for r in all_rows))}")
print(f"Regions ({len(regions)}): {regions}")
print(f"Displacement types: {disps}")
print(f"Metrics: {metrics}")
