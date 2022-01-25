package com.example.demo

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object NumAirportsToDb {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    run(args)
    val end = System.currentTimeMillis()
    println(f"\nElapsed time(sec): ${(end - start) / 1000.0}")
  }

  def run(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("num_airports_to_db").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val params = parseArgs(args)
    println("input_file=%s, show_summary=%s"
      .format(params("input_file"), params("show_summary")))

    val num_airports = spark.read.format("csv").option("header", "true").option("inferSchema", "true")
      .load(params("input_file"))
      .groupBy("STATE")
      .agg(count(col("STATE")).alias("COUNT"))
      .coalesce(1)
      .sortWithinPartitions(col("COUNT").desc, col("STATE"))

    val db_prop = new Properties()
    db_prop.put("user", sys.env("DB_USER"))
    db_prop.put("password", sys.env("DB_PASSWORD"))
    db_prop.put("driver", "oracle.jdbc.driver.OracleDriver")
    num_airports.write.mode("overwrite").option("truncate", true)
      .jdbc(sys.env("DB_JDBCURL"), "NUM_AIRPORTS", db_prop)

    if(params("show_summary").toBoolean){
      num_airports.show()
    }

    spark.close()
  }

  def parseArgs(args: Array[String]): scala.collection.mutable.Map[String, String] = {
    var params = scala.collection.mutable.Map(
      "input_file" -> null, "show_summary" -> "false"
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
