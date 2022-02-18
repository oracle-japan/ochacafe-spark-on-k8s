package org.example.filteredstream

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.streaming.Trigger

import scala.reflect.io.Directory
import java.io.File
import java.util.Properties
import java.io.InputStream
import scala.collection.mutable.ArrayBuffer
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.Namespace
import net.sourceforge.argparse4j.impl.Arguments
import net.sourceforge.argparse4j.inf.FeatureControl
import net.sourceforge.argparse4j.inf.Argument
import net.sourceforge.argparse4j.inf.ArgumentParser
import com.atilika.kuromoji.jumandic.Tokenizer

object FilteredStream {

  val tokenizer = new Tokenizer();

  def main(args: Array[String]): Unit = {

    val db_prop = new Properties()
    db_prop.put("user", sys.env("DB_USER"))
    db_prop.put("password", sys.env("DB_PASSWORD"))
    db_prop.put("driver", "oracle.jdbc.driver.OracleDriver")

    val ns = parseOptions(args)
    ns.getAttrs().forEach((k,v) => println(k + ": " + v))

    val checkpointLocation = ns.getString("checkpointLocation").concat(if(ns.getString("checkpointLocation").endsWith("/")){""}else{"/"})

    val spark = SparkSession
      .builder()
      .appName("SparkFilteredStream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    import org.apache.spark.sql.types._
    import spark.implicits._

    val dataSchema = new StructType()
      .add(name = "id", dataType = StringType, nullable = false)
      .add(name = "text", dataType = StringType, nullable = false)

    val tweetDataSchema = new StructType()
      .add(name = "data", dataType = dataSchema, nullable = false)
      .add(name = "matching_rules", dataType = StringType, nullable = false)
      .add(name = "ts", dataType = TimestampType, nullable = false)

    val tokenizeUDF = udf((x:String) => tokenize(x)).asNondeterministic
    
    val base_stream = 
      spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ns.getString("bootstrap_servers"))
      .option("startingoffsets", "latest")
      .option("failOnDataLoss", false)
      .option("subscribe", ns.getString("topic"))
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", tweetDataSchema).as("tweetData"))
      .select($"tweetData.ts".as("ts"), $"tweetData.data".as("data"))
      .select($"ts", $"data.text".as("text"))

    val keywordsToDatabase =
      base_stream
      .select($"ts", tokenizeUDF($"text").as("tokens"))
      .select($"ts", explode($"tokens").as("token"))
      .withWatermark("ts", ns.getString("watermark"))
      .groupBy(window($"ts", ns.getString("window")), $"token")
      .agg(count("token").as("count"))
      .select(
        $"window.start".as("window_start"), 
        $"window.end".as("window_end"), 
        $"token".as("keyword"), 
        $"count".as("appearances"), 
      )
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write.mode("append")
          .option("truncate", true)
          .option("isolationLevel", "READ_COMMITTED")
          .option("batchsize", 4096)
          .jdbc(sys.env("DB_JDBCURL"), "TWEET_KEYWORDS", db_prop)
      }
      .option("checkpointLocation", checkpointLocation.concat("database-sink"))
      .queryName("keywords to database")
      .start()

    val tweetsToConsole =
      base_stream
      .select($"text")
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", ns.getString("num_output_rows"))
      .option("checkpointLocation", checkpointLocation.concat("console"))
      .queryName("tweets to console")
      .start()

    keywordsToDatabase.awaitTermination()
    tweetsToConsole.awaitTermination()
    spark.close()
  }

  def tokenize(x: String): Array[String] = {

    var words = new ArrayBuffer[String]()

    val tokens = tokenizer.tokenize(x.replaceAll("https://t.co/[0-9a-zA-Z]+", ""))

    if(tokens.size() > 0){
      for (index <- 0 to tokens.size() - 1) {
        val token = tokens.get(index)
        val baseform = token.getBaseForm
        val surface = token.getSurface
        val pos = token.getPartOfSpeechLevel1
        
        if(surface.length() > 32) {
          // skip too long keyword 
          println(s"[$surface] dropped (too long)")
        }else if(!surface.matches("^[0-9a-zA-Zぁ-んーァ-ヶｱ-ﾝﾞﾟ一-龠]+$")) {
          // skip non Japanese 
          //println(s"[$surface] dropped (not Japanese)")
        }else if(surface.matches("^[0-9a-zA-Zぁ-んァ-ヶｱ-ﾝﾞﾟのにはがとてー-十年月日時分秒火水木金土様方\\*]$") 
                    && ! surface.matches("^[亀長黒戸竜西青鬼]$")){
          // stop words
          //println(s"[$surface] dropped (stop words)")
        }else if(pos.equals("名詞")){
          if(surface.matches("^(こと|もの|www|http[s]?|[0-9]+)$")){
            // not much meaningful
            //println(s"[$surface] dropped (not much meaningful)")
          }else{
            //println(s"[$surface] added")
            words += surface
          }
        }else if(pos.equals("動詞") || pos.equals("形容詞")){
          if(baseform.matches("^(する|なる|言う|見る|思う|聞く|いう|やる|行く|いく|できる|出来る|ある|ない|いる|いない|http[s]?|[0-9]+)$")){
            // not much meaningful
            //println(s"[$baseform] dropped (not much meaningful)")
          }else{
            //println(s"[$baseform] added")
            words += baseform
          }
        }
      }
    }
    words.toArray
  }

  def parseOptions(args: Array[String]): Namespace = {
    val parser = ArgumentParsers.newFor("prog").build()
    parser.addArgument("-v", "--verbose").action(Arguments.storeTrue())
    parser.addArgument("--topic")
    addArgument(parser, "--bootstrap-servers", "localhost:9092")
    addArgument(parser, "--window", "30 seconds")
    addArgument(parser, "--watermark", "30 seconds")
    addArgument(parser, "--num-output-rows", "128")
    addArgument(parser, "--checkpointLocation", "file:/tmp")
    parser.parseArgs(args)
  }

  def addArgument(parser: ArgumentParser, argument: String, default: Object): Any = {
    val arg = parser.addArgument(argument)
    arg.getClass().getMethod("setDefault", {new Object().getClass()}).invoke(arg, default)
  }

}

