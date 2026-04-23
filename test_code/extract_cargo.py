"""
Extract cargo class data from EPTMC sheets 7.4 (operations) and 7.5 (tonnes).
Each table is split across 8 sub-pages (p1-p8):
  - Odd pages (p1,p3,p5,p7): first 5 cargo classes
  - Even pages (p2,p4,p6,p8): next 5 cargo classes
  - Page pairs cover different region groups

For each region we extract the interregional expedido and recibido rows
with the full 10-cargo-class breakdown.
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

# 10 cargo classes in order (5 from odd pages, 5 from even pages)
CARGO_CLASSES_LEFT = [
    "Agricolas y animales vivos",
    "Alimenticios y forrajes",
    "Combustibles minerales solidos",
    "Productos petroliferos",
    "Minerales y residuos para refundicion",
]
CARGO_CLASSES_RIGHT = [
    "Productos metalurgicos",
    "Minerales y materiales para construccion",
    "Abonos",
    "Productos quimicos",
    "Maquinas, vehiculos, objetos manufacturados",
]
ALL_CARGO_CLASSES = CARGO_CLASSES_LEFT + CARGO_CLASSES_RIGHT

# Columns for cargo values
# Odd pages: col 1=TOTAL, cols 2-6 = 5 cargo classes
LEFT_CARGO_COLS = list(range(2, 7))
# Even pages: cols 0-4 = 5 cargo classes
RIGHT_CARGO_COLS = list(range(0, 5))

# Page pairs and their region groups
# Each entry: (odd_page_suffix, even_page_suffix, [(region_raw_name_prefix, canonical_name, header_row)])
# header_row is the row of the region name; expedido = header+7, recibido = header+8

REGION_NORMALIZE = {
    "Andaluc": "Andalucia",
    "Arag": "Aragon",
    "Asturias": "Asturias",
    "Balears": "Baleares",
    "Canarias": "Canarias",
    "Cantabria": "Cantabria",
    "Castilla-La Mancha": "Castilla-La Mancha",
    "Castilla y Le": "Castilla y Leon",
    "Catalu": "Cataluna",
    "Comunidad Valenciana": "Com. Valenciana",
    "Extremadura": "Extremadura",
    "Galicia": "Galicia",
    "Madrid": "Madrid",
    "Murcia": "Murcia",
    "Navarra": "Navarra",
    "Pa": "Pais Vasco",  # País Vasco
    "Rioja": "Rioja",
    "Ceuta": "Ceuta y Melilla",
}


def normalize_region(raw):
    raw = raw.strip()
    for prefix, canon in REGION_NORMALIZE.items():
        if raw.startswith(prefix):
            return canon
    return raw


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


def find_region_blocks(sh):
    """Find region header rows by scanning for known patterns.
    Returns list of (row, canonical_name)."""
    blocks = []
    for r in range(16, sh.nrows):
        label = sh.cell_value(r, 0)
        if not isinstance(label, str):
            continue
        label = label.strip()
        if label == "" or label.startswith("(") or label.startswith("Fuente"):
            continue
        # Check if next row is "Total transportado", confirms this is a region header
        if r + 1 < sh.nrows:
            next_label = sh.cell_value(r + 1, 0)
            if isinstance(next_label, str) and "Total transportado" in next_label:
                canon = normalize_region(label)
                blocks.append((r, canon))
    return blocks


def extract_cargo_for_table(wb, table_prefix, year):
    """Extract cargo data for one table (7.4 or 7.5) from one workbook."""
    rows = []

    # Process page pairs: (p1,p2), (p3,p4), (p5,p6), (p7,p8)
    for i in range(1, 8, 2):
        odd_sheet = f"{table_prefix}p{i}"
        even_sheet = f"{table_prefix}p{i+1}"

        sh_odd = wb.sheet_by_name(odd_sheet)
        sh_even = wb.sheet_by_name(even_sheet)

        region_blocks = find_region_blocks(sh_odd)

        for header_row, region in region_blocks:
            expedido_row = header_row + 7
            recibido_row = header_row + 8

            for direction, data_row in [("expedido", expedido_row), ("recibido", recibido_row)]:
                if data_row >= sh_odd.nrows:
                    continue

                # Left page: 5 cargo classes
                for cls_idx, col in enumerate(LEFT_CARGO_COLS):
                    val = clean_value(sh_odd.cell_value(data_row, col))
                    if val is not None:
                        rows.append({
                            "year": year,
                            "region": region,
                            "direction": direction,
                            "cargo_class": CARGO_CLASSES_LEFT[cls_idx],
                            "value": val,
                        })

                # Right page: 5 cargo classes
                for cls_idx, col in enumerate(RIGHT_CARGO_COLS):
                    val = clean_value(sh_even.cell_value(data_row, col))
                    if val is not None:
                        rows.append({
                            "year": year,
                            "region": region,
                            "direction": direction,
                            "cargo_class": CARGO_CLASSES_RIGHT[cls_idx],
                            "value": val,
                        })

    return rows


all_rows_ops = []
all_rows_tonnes = []

for year, filepath in FILES.items():
    print(f"Processing {filepath} ({year})...")
    wb = xlrd.open_workbook(filepath)

    ops = extract_cargo_for_table(wb, "7.4", year)
    tonnes = extract_cargo_for_table(wb, "7.5", year)

    for r in ops:
        r["metric"] = "operations"
    for r in tonnes:
        r["metric"] = "tonnes_thousands"

    all_rows_ops.extend(ops)
    all_rows_tonnes.extend(tonnes)

all_rows = all_rows_ops + all_rows_tonnes
all_rows.sort(key=lambda r: (r["year"], r["metric"], r["region"], r["direction"], r["cargo_class"]))

output_path = "HW6/cargo.csv"
with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["year", "region", "direction", "cargo_class", "metric", "value"])
    writer.writeheader()
    writer.writerows(all_rows)

regions = sorted(set(r["region"] for r in all_rows))
classes = sorted(set(r["cargo_class"] for r in all_rows))
metrics = sorted(set(r["metric"] for r in all_rows))

print(f"\nWrote {len(all_rows)} rows to {output_path}")
print(f"Years: {sorted(set(r['year'] for r in all_rows))}")
print(f"Regions ({len(regions)}): {regions}")
print(f"Cargo classes ({len(classes)}): {classes}")
print(f"Metrics: {metrics}")
print(f"Directions: {sorted(set(r['direction'] for r in all_rows))}")

# Check cargo class taxonomy stability (Technical.md requirement)
print("\n--- Cargo class taxonomy stability check ---")
for year in sorted(set(r["year"] for r in all_rows)):
    year_classes = sorted(set(r["cargo_class"] for r in all_rows if r["year"] == year))
    match = year_classes == sorted(ALL_CARGO_CLASSES)
    print(f"  {year}: {len(year_classes)} classes, match={match}")
