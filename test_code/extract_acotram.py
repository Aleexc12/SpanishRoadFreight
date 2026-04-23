"""
Extract ACOTRAM cost data from .obs files for Articulado Carga General.
Reads block 2 (lines 129-256) using the field positions from Technical.md.
"""

import os
import csv
import glob

VEHICLE_FILE = "Articulado Carga General.obs"
DATA_DIR = "AcotramData"

# Field positions in block 2 (1-indexed line numbers)
DIRECT_FIELDS = {
    "vehicle_type":            129,
    "km_anual":                135,
    "pct_carga":               136,
    "horas_anuales":           137,
    "personal_conduccion_eur": 177,
    "dietas_conductor_eur":    182,
    "seguros_eur":             192,
    "costes_fiscales_eur":     202,
    "fuel_price_per_litre":    206,
    "fuel_consumption_coeff":  207,
    "peajes_eur":              229,
    "urea_eur":                235,
    "costes_indirectos_eur":   240,
    "period":                  256,
}

FORMULA_INPUTS = {
    "vehicle_price_eur":     142,
    "semitrailer_price_eur": 159,
    "interest_rate_pct":     148,
}


def parse_value(raw, field_name):
    """Parse a raw line value into the appropriate type."""
    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        return raw.strip('"')
    if raw.startswith('#') and raw.endswith('#'):
        inner = raw.strip('#')
        if inner in ('TRUE', 'FALSE'):
            return inner == 'TRUE'
        return inner  # dates like 2020-01-31
    try:
        if '.' in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def extract_obs_file(filepath):
    """Extract fields from a single .obs file."""
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()

    if len(lines) < 256:
        print(f"  WARNING: {filepath} has only {len(lines)} lines, skipping")
        return None

    record = {}
    for field, line_num in DIRECT_FIELDS.items():
        record[field] = parse_value(lines[line_num - 1], field)

    for field, line_num in FORMULA_INPUTS.items():
        record[field] = parse_value(lines[line_num - 1], field)

    # Derived fields
    record["km_en_carga"] = record["km_anual"] * record["pct_carga"] / 100

    # Fuel cost (confirmed formula from Technical.md)
    record["combustible_eur"] = (
        record["fuel_price_per_litre"]
        * record["fuel_consumption_coeff"]
        * record["km_anual"]
    )

    return record


# Find all quarterly folders
folders = sorted(glob.glob(os.path.join(DATA_DIR, "*-*")))
all_rows = []

for folder in folders:
    filepath = os.path.join(folder, VEHICLE_FILE)
    if not os.path.exists(filepath):
        print(f"  MISSING: {filepath}")
        continue

    record = extract_obs_file(filepath)
    if record is None:
        continue

    folder_name = os.path.basename(folder)
    print(f"  {folder_name}: period={record['period']}, km={record['km_anual']}, "
          f"fuel_price={record['fuel_price_per_litre']}, "
          f"costes_indirectos={record['costes_indirectos_eur']}")
    all_rows.append(record)

# Sort by period
all_rows.sort(key=lambda r: r["period"])

# Write CSV
output_fields = [
    "period", "vehicle_type", "km_anual", "pct_carga", "km_en_carga",
    "horas_anuales", "personal_conduccion_eur", "dietas_conductor_eur",
    "seguros_eur", "costes_fiscales_eur", "peajes_eur", "urea_eur",
    "costes_indirectos_eur", "vehicle_price_eur", "semitrailer_price_eur",
    "fuel_price_per_litre", "fuel_consumption_coeff", "interest_rate_pct",
    "combustible_eur",
]

output_path = "HW6/acotram.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=output_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(all_rows)

print(f"\nWrote {len(all_rows)} rows to {output_path}")
print(f"Period range: {all_rows[0]['period']} to {all_rows[-1]['period']}")

# Sanity check: fuel price trend
print("\n--- Fuel price per litre trend ---")
for r in all_rows:
    print(f"  {r['period']}: {r['fuel_price_per_litre']:.4f} EUR/l")
