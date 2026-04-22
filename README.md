# Identifying Structural Inefficiencies in the Spanish Road Freight Network

**Course:** 2026 Spring, Processing Big Data for Analytics Applications (NYU)

**Student:** Boyuan Chen (bc4278), solo project

**Platform:** NYU Dataproc, HDFS, Spark Scala, Hive

The project takes six years of Spanish MITMA freight data and produces two things: a ranking of the 210 interregional corridors by structural inefficiency, and a quarterly margin-pressure index that tracks whether carrier costs are outpacing shipper prices, broken down by distance band.

## Directory layout

```
SpanishRoadFreight/
├── README.md
├── ana_code/
│   ├── CorridorInefficiency.scala    Top-20 most inefficient corridors
│   └── MarginPressureIndex.scala     Quarterly MPI per distance band
├── data_ingest/
│   └── Ingest.scala                  Excel/.obs on HDFS -> CSV on HDFS
├── etl_code/bc4278/
│   └── Clean.scala                   HDFS CSV -> cleaned CSV + Hive tables
├── profiling_code/bc4278/
│   ├── CountRecs.scala               Counts, distinct values, nulls
│   └── FirstCode.scala               Mean/median/mode/stddev + 2 cleanings
├── screenshots/                      Per-step run PDFs
└── test_code/                        Unused code: local Python prototypes
                                      used to reverse-engineer the Excel/.obs
                                      layouts before writing Ingest.scala
```

## Raw data

Data collection was manual:

- **EPTMC** (Excel, one workbook per year 2019-2024) — downloaded from the MITMA public statistics portal:
  https://www.transportes.gob.es/el-ministerio/informacion-estadistica/transporte/transporte-de-mercancias-por-carretera/publicaciones-encuesta-permanente-transporte-mercancias-por-carretera/EPTMC2024
- **ACOTRAM** (proprietary `.obs`) — extracted per-quarter from the ACOTRAM3 Windows desktop application published by MITMA. One `Articulado Carga General.obs` per quarter, folder named by date. Covers 2018-01 through 2026-01 (2023-07 absent from MITMA's release).

All raw files bundled here for reproducibility:
https://drive.google.com/drive/folders/10vG5fpQ-F3-FjTRtPxPqd1OWoxPWJd8D?usp=drive_link

The raw files, cleaned CSVs, and Hive tables under database `flota` are **already on HDFS, world-readable**, under `/user/bc4278_nyu_edu/{6_HW,7_HW,final}/`. Read access for `pd2672_nyu_edu` and `adm209_nyu_edu` was requested via HPC@nyu.edu during HW6 and remains in place.

**Recommended path:** skip the ingest and ETL steps and read the Hive tables directly (see `Inspect results` below). The analytic outputs `flota.corridors_ranking` and `flota.mpi` are already populated. Re-running the full pipeline requires re-uploading the raw data into your own HDFS home, since `Ingest.scala` uses relative paths that resolve to the runner's home directory.

Only if reproducing end-to-end:

```
# 1. Download the Drive folder above onto the Dataproc node's local filesystem.
# 2. Upload both subfolders to your HDFS home:
hdfs dfs -mkdir -p 6_HW/EPTMC 6_HW/AcotramData
hdfs dfs -put EPTMC/*.xls 6_HW/EPTMC/
hdfs dfs -put -r AcotramData/* 6_HW/AcotramData/
```

Add `-f` to the `-put` commands if the destination files already exist.

Post-ingest row counts (to verify ingest succeeded):

| dataset        | rows  |
| -------------- | ----: |
| corridors      | 4,863 |
| regional       | 2,844 |
| cargo          | 3,478 |
| price_index    | 144   |
| distance_bands | 420   |
| acotram        | 31    |

## How to run (end-to-end on Dataproc)

No build step. Upload raw data first (see section above), then run these in order from the project root:

```
# 1. Ingest raw Excel + .obs into HDFS CSV (needs Apache POI)
spark-shell --master yarn --deploy-mode client \
  --packages org.apache.poi:poi:5.2.5 \
  -i data_ingest/Ingest.scala

# 2. Profile the raw data
spark-shell --master yarn --deploy-mode client \
  -i profiling_code/bc4278/CountRecs.scala

# 3. Clean and create Hive tables under database `flota`
spark-shell --master yarn --deploy-mode client \
  -i etl_code/bc4278/Clean.scala

# 4. Profile the cleaned data
spark-shell --master yarn --deploy-mode client \
  --conf spark.dataset.path=6_HW/cleaned \
  -i profiling_code/bc4278/CountRecs.scala

# 5. First-code stats plus 2 cleanings (mean/median/mode, stddev, date split, binary col)
spark-shell --master yarn --deploy-mode client \
  -i profiling_code/bc4278/FirstCode.scala

# 6. Analytics
spark-shell --master yarn --deploy-mode client \
  -i ana_code/CorridorInefficiency.scala
spark-shell --master yarn --deploy-mode client \
  -i ana_code/MarginPressureIndex.scala
```

`--master yarn` is needed for steps 3 and 6 so Hive warehouse paths resolve to HDFS instead of the local filesystem.

## Where results go

Each script writes to HDFS under the runner's home directory; Clean, FirstCode, and the two analytics also register Hive tables in database `flota` (warehouse at `hdfs:///user/bc4278_nyu_edu/flota.db` for the owner account). All sinks use `.mode("overwrite")`, so re-running any step replaces its previous output cleanly.

| Script                     | HDFS path                           | Hive table (in `flota`) |
| -------------------------- | ----------------------------------- | ----------------------- |
| Ingest.scala               | `6_HW/ingested/<name>/`             | —                       |
| Clean.scala                | `6_HW/cleaned/<name>/`              | `<name>`                |
| FirstCode.scala            | `7_HW/firstcode/acotram_enriched/`  | `acotram_enriched`      |
| CorridorInefficiency.scala | `final/analytic/corridors_ranking/` | `corridors_ranking`     |
| MarginPressureIndex.scala  | `final/analytic/mpi/`               | `mpi`                   |

`<name>` ∈ `{corridors, regional, cargo, price_index, distance_bands, acotram}`.
`FirstCode.scala` adds derived columns `year`, `quarter`, `fuel_shock` to its table.

## Inspect results

```
spark.sql("SELECT origin, destination, round(composite, 3) AS score " +
          "FROM flota.corridors_ranking ORDER BY composite DESC LIMIT 20").show(false)

spark.sql("SELECT distance_band, round(avg(mpi), 2) AS avg_mpi " +
          "FROM flota.mpi GROUP BY distance_band ORDER BY avg_mpi").show(false)
```

## Analytic details

### CorridorInefficiency.scala

Each directed pair `(origin, destination)` over 2019-2024 gets four metrics.
Each is min-max normalized to `[0,1]` and the four normalized values are summed into a composite score:

- `asymmetry`: `|tonnes_AB - tonnes_BA| / (tonnes_AB + tonnes_BA)`, averaged across years. A 3:1 directional ratio maps to about 0.50.
- `empty_rate`: `operations_empty / operations_total`, averaged across years.
- `instability`: coefficient of variation of yearly `tonnes_thousands`.
- `concentration`: HHI of the origin region's `expedido` cargo mix across the 10 cargo classes, averaged across years. HHI sits at the origin level because the cargo table is published per region, not per O/D pair.

All 210 directed pairs are written; the top 20 print to the console.

### MarginPressureIndex.scala

For every `(year, quarter, distance_band)`:

```
MPI = (price_idx / cost_idx) * 100    both rebased to 2019 average = 100
```

MPI < 100 means carrier costs rose faster than the prices shippers paid on
that segment.

Cost side: the 8 extracted ACOTRAM euro columns are summed and divided by `km_anual`. Those 8 columns (fuel, driver wages, tolls, insurance, fiscal costs, urea, overhead, subsistence) cover the volatile part of the envelope, roughly 75-80% of total operating cost. Because both series are rebased to 2019 = 100, the slow-moving categories left out of ingestion have little effect on the index direction.

The 2023 Q3 ACOTRAM gap (noted in Raw data) surfaces as a null row rather than a dropped row, so it stays visible in the output.

## Screenshots

PDFs in `screenshots/` document each step:

| file                   | content                                   |
| ---------------------- | ----------------------------------------- |
| ss_ingest.pdf          | Ingest.scala run log                      |
| ss_countRecs_raw.pdf   | CountRecs.scala on raw ingested data      |
| ss_clean.pdf           | Clean.scala run + Hive table creation     |
| ss_countRecs_clean.pdf | CountRecs.scala on cleaned data           |
| ss_firstcode.pdf       | FirstCode.scala run (stats + 2 cleanings) |
| ss_corridorInef.pdf    | CorridorInefficiency.scala run            |
| ss_MPI.pdf             | MarginPressureIndex.scala run             |
