import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// ============================================================
// Corridor Inefficiency Ranking
// ============================================================
// For each directed origin->destination pair over 2019-2024,
// computes 4 structural inefficiency metrics plus a composite.
// All metrics min-max normalized to [0,1]; higher = more inefficient.
//
//   asymmetry     : |tonnes_AB - tonnes_BA| / (tonnes_AB + tonnes_BA), yearly avg
//   empty_rate    : ops_empty / ops_total, yearly avg
//   instability   : stddev(tonnes_yearly) / mean(tonnes_yearly) (CV across years)
//   concentration : HHI of origin's expedido cargo mix, yearly avg
//
// composite = sum of the four _n columns (equal weights).
// Output: ranked table + top 20 printed; CSV and Hive table written.
//
// Limitations / governance:
//   - HHI computed at origin region (cargo table 7.4/7.5 is published per
//     region, not per O/D pair). See design note near hhiPerOrigin.
//   - Equal weights across the 4 components. Could be learned from labeled
//     inefficient corridors, but no public ground-truth labels exist.
//   - Baleares / Canarias / Ceuta y Melilla excluded at ingest time
//     (Ingest.scala line ~175, `excluded` set in the corridors loop).
//     Peninsular-only: 15 CCAA = 210 directed pairs.
// ============================================================

val corridors = spark.table("flota.corridors")
val cargo     = spark.table("flota.cargo")

// ---- Pivot corridors so each (year, a, b) is one row with metric cols
val wide = corridors
  .groupBy($"year", $"origin_region".as("a"), $"destination_region".as("b"))
  .pivot("metric", Seq("transport_operations_total", "transport_operations_empty", "tonnes_thousands"))
  .agg(first("value"))
  .withColumnRenamed("transport_operations_total", "ops_total")
  .withColumnRenamed("transport_operations_empty", "ops_empty")
  .withColumnRenamed("tonnes_thousands", "tonnes_ab")
  .cache()   // reused twice: once for `yearly`, once via `reverse` self-join

// ---- Attach reverse-direction tonnes for asymmetry
val reverse = wide.select($"year", $"b".as("a"), $"a".as("b"), $"tonnes_ab".as("tonnes_ba"))

val yearly = wide
  .join(reverse, Seq("year", "a", "b"), "left")
  .withColumn("tonnes_ba", coalesce($"tonnes_ba", lit(0.0)))
  .withColumn("asym_year",
    when(($"tonnes_ab" + $"tonnes_ba") > 0,
      abs($"tonnes_ab" - $"tonnes_ba") / ($"tonnes_ab" + $"tonnes_ba")))
  .withColumn("empty_year",
    when($"ops_total" > 0, $"ops_empty" / $"ops_total"))

// ---- Aggregate across 2019-2024 per (a, b)
val perPair = yearly
  .groupBy($"a", $"b")
  .agg(
    avg($"asym_year").as("asymmetry"),
    avg($"empty_year").as("empty_rate"),
    (stddev($"tonnes_ab") / avg($"tonnes_ab")).as("instability")
  )

// ---- HHI of cargo concentration per origin (expedido tonnes, yearly avg)
// Proxy metric: EPTMC 7.4 / 7.5 cargo tables are aggregated per (region,
// direction), with no destination dimension. An O/D-level HHI is therefore
// not recoverable from this data source. Any per-destination split would
// just replicate the origin's HHI across all destinations. Kept at origin
// level so the limitation is explicit in the schema of the output.
val cargoTonnes = cargo
  .filter($"direction" === "expedido" && $"metric" === "tonnes_thousands" && $"value" > 0)
  .select($"year", $"region", $"cargo_class", $"value".as("tonnes"))

val cargoYearTotal = cargoTonnes.groupBy($"year", $"region").agg(sum("tonnes").as("total"))

val hhiYear = cargoTonnes
  .join(cargoYearTotal, Seq("year", "region"))
  .withColumn("share", $"tonnes" / $"total")
  .groupBy($"year", $"region")
  .agg(sum(pow($"share", 2)).as("hhi_year"))

val hhiPerOrigin = hhiYear
  .groupBy($"region")
  .agg(avg("hhi_year").as("concentration"))

val scored = perPair
  .join(hhiPerOrigin, $"a" === $"region", "left")
  .drop("region")
  .cache()   // reused 4x by the normalization foldLeft (one min/max action per metric)

// ---- Min-max normalize each metric to [0,1]
// Min-max chosen over z-score so the composite is bounded in [0, 4] and
// readable as "fraction of worst across the 4 dimensions." Z-score would
// give an unbounded composite and negative values for balanced corridors,
// which would invert the ranking's intuitive direction.
//
// Earlier version used z-score:
//   def zScoreNorm(df: DataFrame, c: String): DataFrame = {
//     val stats = df.agg(avg(c).as("mu"), stddev(c).as("sd")).first
//     val mu = stats.getDouble(0); val sd = stats.getDouble(1)
//     df.withColumn(s"${c}_n", (col(c) - lit(mu)) / lit(sd))
//   }
// Abandoned: produced negative composites for balanced corridors and an
// unbounded upper tail, so the composite lost its "fraction of worst"
// interpretation. Min-max keeps every component in [0,1] and the sum in [0,4].
def minMaxNorm(df: DataFrame, c: String): DataFrame = {
  val bounds = df.agg(min(c).cast("double").as("mn"), max(c).cast("double").as("mx")).first
  val mn = if (bounds.isNullAt(0)) 0.0 else bounds.getDouble(0)
  val mx = if (bounds.isNullAt(1)) 0.0 else bounds.getDouble(1)
  val range = mx - mn
  val expr = if (range == 0.0) lit(0.0) else (col(c) - lit(mn)) / lit(range)
  df.withColumn(s"${c}_n", expr)
}

val normalized = Seq("asymmetry", "empty_rate", "instability", "concentration")
  .foldLeft(scored)((df, m) => minMaxNorm(df, m))

// ---- Composite score (equal weights; nulls treated as 0 contribution)
val ranked = normalized
  .withColumn("composite",
    coalesce($"asymmetry_n", lit(0.0)) +
    coalesce($"empty_rate_n", lit(0.0)) +
    coalesce($"instability_n", lit(0.0)) +
    coalesce($"concentration_n", lit(0.0)))
  .select(
    $"a".as("origin"),
    $"b".as("destination"),
    $"asymmetry", $"empty_rate", $"instability", $"concentration",
    $"asymmetry_n", $"empty_rate_n", $"instability_n", $"concentration_n",
    $"composite")
  .orderBy(desc("composite"))

println("Top 20 most inefficient corridors:")
ranked.show(20, false)

ranked.coalesce(1).write.mode("overwrite").option("header", "true").csv("final/analytic/corridors_ranking")
ranked.write.mode("overwrite").saveAsTable("flota.corridors_ranking")

println("Hive table flota.corridors_ranking:")
spark.sql("DESCRIBE flota.corridors_ranking").show(false)
spark.sql("SELECT origin, destination, round(composite, 3) AS score FROM flota.corridors_ranking ORDER BY composite DESC LIMIT 20").show(false)

System.exit(0)
