import org.apache.spark.sql.functions._
val hdfsBase = spark.conf.getOption("spark.dataset.path").getOrElse("6_HW/ingested")
println(s">> Profiling data from: ${hdfsBase}")

// 1. CORRIDORS
println("== CORRIDORS ==")
val corridors = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/corridors")
println(s"Total records: ${corridors.count()}")
corridors.printSchema()
corridors.rdd.map(r => (r.getAs[String]("metric"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2).foreach { case (k,v) => println(s"  $k -> $v") }
corridors.select("year").distinct().orderBy("year").show(50,false)
corridors.select("origin_region").distinct().orderBy("origin_region").show(50,false)
corridors.select("destination_region").distinct().orderBy("destination_region").show(50,false)
corridors.select("metric").distinct().show(50,false)
corridors.columns.foreach { c => println(s"  $c: ${corridors.select(c).distinct().count()} distinct") }
corridors.columns.foreach { c => println(s"  $c: ${corridors.filter(col(c).isNull || col(c)==="" || col(c)==="..").count()} nulls") }
corridors.select(min("value"),max("value"),avg("value")).show()

// 2. REGIONAL
println("== REGIONAL ==")
val regional = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/regional")
println(s"Total records: ${regional.count()}")
regional.printSchema()
regional.rdd.map(r => (r.getAs[String]("displacement_type"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2).foreach { case (k,v) => println(s"  $k -> $v") }
regional.select("region").distinct().orderBy("region").show(50,false)
regional.select("displacement_type").distinct().show(50,false)
regional.select("metric").distinct().show(50,false)
regional.columns.foreach { c => println(s"  $c: ${regional.select(c).distinct().count()} distinct") }
regional.columns.foreach { c => println(s"  $c: ${regional.filter(col(c).isNull || col(c)==="").count()} nulls") }
regional.select(min("value"),max("value"),avg("value")).show()

// 3. CARGO
println("== CARGO ==")
val cargo = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/cargo")
println(s"Total records: ${cargo.count()}")
cargo.printSchema()
cargo.rdd.map(r => (r.getAs[String]("cargo_class"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2).foreach { case (k,v) => println(s"  $k -> $v") }
cargo.select("cargo_class").distinct().orderBy("cargo_class").show(50,false)
cargo.select("direction").distinct().show(50,false)
cargo.select("region").distinct().orderBy("region").show(50,false)
cargo.columns.foreach { c => println(s"  $c: ${cargo.select(c).distinct().count()} distinct") }
cargo.columns.foreach { c => println(s"  $c: ${cargo.filter(col(c).isNull || col(c)==="").count()} nulls") }
cargo.select(min("value"),max("value"),avg("value")).show()

// 4. PRICE INDEX
println("== PRICE INDEX ==")
val priceIndex = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/price_index")
println(s"Total records: ${priceIndex.count()}")
priceIndex.printSchema()
priceIndex.rdd.map(r => (r.getAs[String]("distance_band"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2).foreach { case (k,v) => println(s"  $k -> $v") }
priceIndex.select("year").distinct().orderBy("year").show(50,false)
priceIndex.select("quarter").distinct().orderBy("quarter").show(50,false)
priceIndex.select("distance_band").distinct().show(50,false)
priceIndex.select(min("price_index"),max("price_index"),avg("price_index")).show()

// 5. DISTANCE BANDS
println("== DISTANCE BANDS ==")
val distBands = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/distance_bands")
println(s"Total records: ${distBands.count()}")
distBands.printSchema()
distBands.rdd.map(r => (r.getAs[String]("distance_band"), 1)).reduceByKey(_ + _).collect().sortBy(-_._2).foreach { case (k,v) => println(s"  $k -> $v") }
distBands.select("distance_band").distinct().show(50,false)
distBands.select("service_type").distinct().show(50,false)
distBands.select("metric").distinct().show(50,false)
distBands.select(min("value"),max("value"),avg("value")).show()

// 6. ACOTRAM
println("== ACOTRAM ==")
val acotram = spark.read.option("header","true").option("inferSchema","true").csv(s"${hdfsBase}/acotram")
println(s"Total records: ${acotram.count()}")
acotram.printSchema()
acotram.withColumn("year",substring(col("period").cast("string"),1,4)).rdd.map(r=>(r.getAs[String]("year"),1)).reduceByKey(_+_).collect().sortBy(_._1).foreach{case(k,v)=>println(s"  $k -> $v")}
acotram.select("period").distinct().orderBy("period").show(50,false)
if (acotram.columns.contains("vehicle_type")) acotram.select("vehicle_type").distinct().show(50,false)
acotram.select(min("fuel_price_per_litre"),max("fuel_price_per_litre"),avg("fuel_price_per_litre")).show()
Seq("personal_conduccion_eur","dietas_conductor_eur","seguros_eur","costes_fiscales_eur","combustible_eur").foreach{c=>val s=acotram.select(min(c),max(c),avg(c)).first();println(s"  $c: min=${s.get(0)}, max=${s.get(1)}, avg=${s.get(2)}")}
acotram.columns.foreach{c=>println(s"  $c: ${acotram.filter(col(c).isNull||col(c)==="").count()} nulls")}

println("== PROFILING COMPLETE ==")
