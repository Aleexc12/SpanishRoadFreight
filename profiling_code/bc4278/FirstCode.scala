import org.apache.spark.sql.functions._

val acotram = spark.table("flota.acotram")
val priceIndex = spark.table("flota.price_index")
val corridors = spark.table("flota.corridors")

def stats(df: org.apache.spark.sql.DataFrame, col: String) = {
  val mean = df.select(avg(col)).first.getDouble(0)
  val median = df.stat.approxQuantile(col, Array(0.5), 0.0001)(0)
  val mode = df.groupBy(col).count.orderBy(desc("count")).first.get(0)
  println(s"$col -> mean=$mean median=$median mode=$mode")
}

println("acotram")
stats(acotram, "fuel_price_per_litre")
stats(acotram, "combustible_eur")
stats(acotram, "personal_conduccion_eur")
stats(acotram, "peajes_eur")

println("price_index")
stats(priceIndex, "price_index")

println("corridors (transport_operations_total)")
val ops = corridors.filter($"metric" === "transport_operations_total")
stats(ops, "value")

val sd = acotram.select(stddev("combustible_eur")).first.getDouble(0)
println("stddev(combustible_eur) = " + sd)

// Cleaning 1: date formatting. Split period into year and quarter so acotram
// can be joined against price_index for the Margin Pressure Index.
// Cleaning 2: binary column. fuel_shock = true when fuel price > 1.50 EUR/l
// (flags the 2022 diesel spike that peaked at 1.97).

val enriched = (acotram
  .withColumn("year", year($"period"))
  .withColumn("quarter", concat(lit("Q"), quarter($"period")))
  .withColumn("fuel_shock", $"fuel_price_per_litre" > 1.50))

enriched.select("period", "year", "quarter", "fuel_price_per_litre", "fuel_shock").orderBy("period").show(false)
enriched.groupBy("fuel_shock").count.show()

enriched.coalesce(1).write.mode("overwrite").option("header", "true").csv("7_HW/firstcode/acotram_enriched")
enriched.write.mode("overwrite").saveAsTable("flota.acotram_enriched")

spark.sql("DESCRIBE flota.acotram_enriched").show(false)
