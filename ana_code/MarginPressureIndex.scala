import org.apache.spark.sql.functions._

// ============================================================
// Margin Pressure Index (MPI)
// ============================================================
// Per (year, quarter, distance_band):
//   MPI = (price_idx_rebased / cost_idx_rebased) * 100
// Both series rebased to 2019 average = 100.
// MPI < 100 => carrier costs rising faster than the price shippers pay
// on that distance segment (margin compression).
//
// Cost side: sum of 8 extracted ACOTRAM annual euro columns / km_anual.
//   Missing from ingestion (amortizacion, financiacion, neumaticos,
//   mantenimiento, reparaciones) per proposal schema note. The captured
//   columns cover the most volatile ~75-80% of TOC (fuel, driver,
//   tolls), so a rebased index still tracks pressure direction.
// Price side: flota.price_index, rebased per distance_band to 2019=100.
// Missing ACOTRAM quarter (2023 Q3) left as null via left join.
//
// Limitations / governance:
//   - Articulado Carga General only (see Ingest.scala reading one .obs
//     file per quarter). ACOTRAM publishes cost models for other vehicle
//     configurations; using only the articulated profile biases MPI toward
//     long-haul dynamics, since short-haul urban delivery is typically run
//     on rigid trucks with a different cost curve. The <=50 km band should
//     therefore be read as "price pressure on shippers of short-haul
//     contracts measured against long-haul cost trends," not as a true
//     short-haul MPI.
//   - 2023 Q3 left as null, not imputed. MITMA did not publish the quarter.
//     Linear interpolation would hide the data gap from downstream consumers;
//     keeping the null surfaces it in the plot.
//   - Rebasing to 2019 avg = 100 assumes 2019 is a "normal" reference.
//     Validated post-hoc by the flat MPI through 2019-early 2021. If 2019
//     had been anomalous, the pre-squeeze baseline would not sit at 100.
// ============================================================

val acotram    = spark.table("flota.acotram")
val priceIndex = spark.table("flota.price_index")

// ---- Cost side: total operating cost / km
val costCols = Seq(
  "personal_conduccion_eur", "dietas_conductor_eur", "seguros_eur",
  "costes_fiscales_eur", "peajes_eur", "urea_eur",
  "costes_indirectos_eur", "combustible_eur"
)
val totalCostExpr = costCols.map(col).reduce(_ + _)

val cost = acotram
  .withColumn("year", year($"period"))
  .withColumn("quarter", concat(lit("Q"), quarter($"period")))
  .withColumn("total_cost_eur", totalCostExpr)
  .withColumn("cost_per_km", $"total_cost_eur" / $"km_anual")
  .select($"year", $"quarter", $"total_cost_eur", $"km_anual", $"cost_per_km")

// ---- Rebase cost to 2019 average = 100
val costBase = cost.filter($"year" === 2019).agg(avg("cost_per_km")).first.getDouble(0)
println(f"Cost base (2019 avg cost/km): $costBase%.4f EUR/km")

val costIdx = cost.withColumn("cost_idx", $"cost_per_km" / lit(costBase) * 100)

// ---- Rebase price_index per band to 2019 = 100
val priceBase = priceIndex.filter($"year" === 2019)
  .groupBy($"distance_band").agg(avg("price_index").as("price_base"))

val priceIdx = priceIndex
  .join(priceBase, "distance_band")
  .withColumn("price_idx", $"price_index" / $"price_base" * 100)
  .select($"year", $"quarter", $"distance_band", $"price_idx")

// ---- Join: every (band, year, quarter) joins cost. Null cost_idx -> null MPI
// Left join (not inner) so the 2023-Q3 gap stays visible as a null MPI
// value. Broadcasting the cost side: ~23 quarterly rows vs ~120 in the
// price panel. Broadcast hint avoids a shuffle on the join key.
//
// Earlier version used an inner join and dropped nulls:
//   val mpi = priceIdx
//     .join(costIdx.select($"year", $"quarter", $"cost_idx"), Seq("year", "quarter"))
//     .withColumn("mpi", $"price_idx" / $"cost_idx" * 100)
//     .filter($"mpi".isNotNull)
// Abandoned: silently hid the 2023-Q3 MITMA data gap from the output and
// the plots, which made the gap look like a continuous series. Switched
// to left join + broadcast so the null row is preserved and visible, and
// added the broadcast hint once the cost side was confirmed tiny.
val mpi = priceIdx
  .join(broadcast(costIdx.select($"year", $"quarter", $"cost_idx")), Seq("year", "quarter"), "left")
  .withColumn("mpi", $"price_idx" / $"cost_idx" * 100)
  .filter($"year".between(2019, 2024))
  .select($"year", $"quarter", $"distance_band", $"price_idx", $"cost_idx", $"mpi")
  .orderBy($"distance_band", $"year", $"quarter")

println("MPI full panel (year, quarter, distance_band):")
mpi.show(200, false)

println("Average MPI per distance band over 2019-2024 (values < 100 = margin squeezed):")
mpi.groupBy($"distance_band")
  .agg(round(avg("mpi"), 2).as("avg_mpi"),
       round(min("mpi"),  2).as("min_mpi"),
       round(max("mpi"),  2).as("max_mpi"))
  .orderBy("avg_mpi")
  .show(false)

mpi.coalesce(1).write.mode("overwrite").option("header", "true").csv("final/analytic/mpi")
mpi.write.mode("overwrite").saveAsTable("flota.mpi")

println("Hive table flota.mpi:")
spark.sql("DESCRIBE flota.mpi").show(false)

System.exit(0)
