package com.example.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object NumAirportsByState {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    run(args)
    val end = System.currentTimeMillis()
    println(f"\nElapsed time(sec): ${(end - start) / 1000.0}")
  }

  def run(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("num_airports_by_state").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val params = parseArgs(args)
    println("input_file=%s, output_file=%s, output_format=%s, show_summary=%s"
      .format(params("input_file"), params("output_file"), params("output_format"), params("show_summary")))

    val num_airports_by_state = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load(params("input_file"))
      .groupBy("STATE")
      .agg(count(col("STATE")).alias("COUNT"))
      .coalesce(1)
      .sortWithinPartitions(col("COUNT").desc, col("STATE"))

    num_airports_by_state.write.format(params("output_format")).mode("overwrite").save(params("output_file"))

    if(params("show_summary").toBoolean){
      num_airports_by_state.show()
    }

    spark.close()
  }

  def parseArgs(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    var params = scala.collection.mutable.Map(
      "input_file" -> null, "output_file" -> null, "output_format" -> "json", "show_summary" -> "false"
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
