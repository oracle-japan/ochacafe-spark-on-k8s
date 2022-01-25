package com.example.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object AvgArrivalDelayByAirline {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    run(args)
    val end = System.currentTimeMillis()
    println(f"\nElapsed time(sec): ${(end - start) / 1000.0}")
  }

  def run(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("avg_delay_by_airline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val params = parseArgs(args)
    println("input_dir=%s, output_dir=%s, output_format=%s, show_summary=%s"
      .format(params("input_dir"), params("output_dir"), params("output_format"), params("show_summary")))

    val airlines = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load(s"${params("input_dir")}/airlines.csv")
      .select(col("IATA_CODE"), col("AIRLINE").alias("AIRLINE_NAME"))

    val flights = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load(s"${params("input_dir")}/flights.csv")

    val avg_arrival_delay = flights.groupBy("AIRLINE")
      .agg(count(col("AIRLINE")).alias("FLIGHTS_COUNT"), avg(col("ARRIVAL_DELAY")).alias("AVG_ARRIVAL_DELAY"))
      .withColumn("AVG_ARRIVAL_DELAY_BY_MINUTES", format_number(col("AVG_ARRIVAL_DELAY"), 2))
      .join(airlines, flights.col("AIRLINE") === airlines.col("IATA_CODE"), "left")
      .coalesce(1)
      .sortWithinPartitions("AVG_ARRIVAL_DELAY")
      .select(col("AIRLINE_NAME"), col("FLIGHTS_COUNT"), col("AVG_ARRIVAL_DELAY_BY_MINUTES"))

    avg_arrival_delay.write.format(params("output_format")).mode("overwrite")
      .save(s"${params("output_dir")}/avg_arrival_delay_by_airline")

    if(params("show_summary").toBoolean){
      var num_flights: Long = 0
      for (row <- avg_arrival_delay.collect()) {
        num_flights += row.getAs[Long]("FLIGHTS_COUNT")
        println("%-32s %10d %8s".format(row.getAs("AIRLINE_NAME"), row.getAs[Long]("FLIGHTS_COUNT"), row.getAs("AVG_ARRIVAL_DELAY_BY_MINUTES")))
      }
      println("Total %,d flights.".format(num_flights))
    }

    spark.close()
  }

  def parseArgs(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    var params = scala.collection.mutable.Map(
      "input_dir" -> null, "output_dir" -> null, "output_format" -> "json", "show_summary" -> "false"
    )
    val keys = params.keys
    var i = 0
    while (i < args.length) {
      for (key <- keys) {
        if (args(i) == "--" + key) {
          i = i + 1
          params(key) = args(i)
        }
      }
      i = i + 1
    }
    params
  }

}
