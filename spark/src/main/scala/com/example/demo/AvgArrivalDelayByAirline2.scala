package com.example.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object AvgArrivalDelayByAirline2 {

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

    airlines.createOrReplaceTempView("AIRLINES")
    flights.createOrReplaceTempView("FLIGHTS")
    val avg_arrival_delay = spark.sql("""
        SELECT first(a.AIRLINE_NAME) as AIRLINE_NAME, count(f.AIRLINE) as FLIGHTS_COUNT, avg(f.ARRIVAL_DELAY) as AVG_ARRIVAL_DELAY
          FROM FLIGHTS as f LEFT JOIN AIRLINES as a ON f.AIRLINE = a.IATA_CODE 
            GROUP BY f.AIRLINE
             ORDER BY AVG_ARRIVAL_DELAY
    """)
    avg_arrival_delay.show()

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
