import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val hdfsBase = "6_HW/ingested"
val outBase  = "6_HW/cleaned"

// Governance note: the filters in this file define the analytical universe.
//   - origin != destination drops self-loops. Intrarregional flows live in
//     a separate EPTMC table family (6.1-6.4, ingested into `regional`) and
//     aren't part of the corridor analysis.
//   - year between 2019 and 2024 matches the six EPTMC files we ingested
//     (see Ingest.scala `eptmcFiles` map). The filter is a belt-and-braces
//     guard in case a stray year sneaks in; it is not a substantive
//     analytical cutoff.
//   - Baleares / Canarias / Ceuta y Melilla are not listed here because
//     they are excluded upstream in Ingest.scala (line ~175 `excluded` set
//     in the corridors loop). Peninsular-only: 15 CCAA = 210 directed pairs.

// CORRIDORS
val corridorsRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/corridors")
println(s"corridors raw: ${corridorsRaw.count()}")
val corridorsClean = corridorsRaw.filter(col("value").isNotNull).filter(col("origin_region").isNotNull && col("destination_region").isNotNull).filter(col("origin_region")=!=col("destination_region")).filter(col("year").between(2019,2024))
println(s"corridors cleaned: ${corridorsClean.count()}")
corridorsClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/corridors")

// REGIONAL
val regionalRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/regional")
println(s"regional raw: ${regionalRaw.count()}")
val regionalClean = regionalRaw.filter(col("value").isNotNull).filter(col("region").isNotNull).filter(col("year").between(2019,2024))
println(s"regional cleaned: ${regionalClean.count()}")
regionalClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/regional")

// CARGO
val cargoRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/cargo")
println(s"cargo raw: ${cargoRaw.count()}")
val cargoClean = cargoRaw.filter(col("value").isNotNull && col("value")>0).filter(col("region").isNotNull).filter(col("cargo_class").isNotNull).filter(col("year").between(2019,2024))
println(s"cargo cleaned: ${cargoClean.count()}")
cargoClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/cargo")

// PRICE INDEX
val priceRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/price_index")
println(s"price_index raw: ${priceRaw.count()}")
val priceClean = priceRaw.filter(col("price_index").isNotNull).filter(col("year").between(2019,2024))
println(s"price_index cleaned: ${priceClean.count()}")
priceClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/price_index")

// DISTANCE BANDS
val distRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/distance_bands")
println(s"distance_bands raw: ${distRaw.count()}")
val distClean = distRaw.filter(col("value").isNotNull && col("value")>0).filter(col("distance_band").isNotNull)
println(s"distance_bands cleaned: ${distClean.count()}")
distClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/distance_bands")

// ACOTRAM
val acotramRaw = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/acotram")
println(s"acotram raw: ${acotramRaw.count()}")
val acotramClean = acotramRaw.drop("vehicle_type","vehicle_price_eur","semitrailer_price_eur","interest_rate_pct").withColumn("year",substring(col("period").cast("string"),1,4).cast(IntegerType)).filter(col("year").between(2019,2024)).drop("year").filter(col("period").isNotNull)
println(s"acotram cleaned: ${acotramClean.count()}")
acotramClean.write.mode("overwrite").option("header","true").csv(s"${outBase}/acotram")

// SUMMARY
Seq("corridors","regional","cargo","price_index","distance_bands","acotram").foreach{ds=>val df=spark.read.option("header","true").option("inferSchema","true").csv(s"${outBase}/${ds}");println(s"  $ds: ${df.count()} records, ${df.columns.length} columns")}

// HIVE
spark.sql("CREATE DATABASE IF NOT EXISTS flota LOCATION 'hdfs:///user/bc4278_nyu_edu/flota.db'")
Seq("corridors","regional","cargo","price_index","distance_bands","acotram").foreach { ds =>
  spark.read.option("header","true").option("inferSchema","true").csv(s"${outBase}/${ds}").write.mode("overwrite").saveAsTable(s"flota.${ds}")
}
spark.sql("SHOW TABLES IN flota").show()
Seq("corridors","regional","cargo","price_index","distance_bands","acotram").foreach { ds =>
  println(s"== ${ds} ==")
  spark.sql(s"DESCRIBE flota.${ds}").show()
}
