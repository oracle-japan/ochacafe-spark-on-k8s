import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, ArrayType
from pyspark.sql.functions import count, col, udf, from_json, window, explode, from_utc_timestamp

import tweet_tokenizer

def foreach_batch(batch_df, batch_id):
    #batch_df.show()
    batch_df.write \
    .option('truncate', True) \
    .option('isolationLevel', 'READ_COMMITTED') \
    .option('batchsize', 4096) \
    .jdbc(os.environ['DB_JDBCURL'], 'TWEET_KEYWORDS', 'append', {
        'user' : os.environ['DB_USER'], 
        'password' : os.environ['DB_PASSWORD'], 
        'driver' : 'oracle.jdbc.driver.OracleDriver' 
    })

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default='localhost:9092')
    parser.add_argument('--topic', default='my-topic')
    parser.add_argument('--window', default='30 seconds')
    parser.add_argument('--watermark', default='30 seconds')
    parser.add_argument('--num-output-rows', default='128')
    parser.add_argument('--checkpoint-location', default='file:/tmp/spark/checlpoint')
    args = parser.parse_args()

    checkpoint_location = args.checkpoint_location + '/' if not args.checkpoint_location.endswith('/') else ''

    tokenize_udf = udf(tweet_tokenizer.tokenize, ArrayType(StringType())).asNondeterministic()

    spark = SparkSession.builder.appName("spark-filtered-stream_py").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dataSchema = StructType() \
      .add("id", StringType(), False) \
      .add("text", StringType(), False)

    tweetDataSchema = StructType() \
      .add("data", dataSchema, False) \
      .add("matching_rules", StringType(), False) \
      .add("ts", TimestampType(), False)

    base_stream = spark.readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", args.bootstrap_servers) \
      .option("startingoffsets", "latest") \
      .option("failOnDataLoss", False) \
      .option("subscribe", args.topic) \
      .load() \
      .selectExpr("CAST(value AS STRING)") \
      .select(from_json("value", tweetDataSchema).alias("tweetData")) \
      .select(col("tweetData.ts").alias("ts"), col("tweetData.data").alias("data")) \
      .select("ts", col("data.text").alias("text")) \

    keywordsToDatabase = base_stream \
      .select("ts", tokenize_udf("text").alias("tokens")) \
      .select("ts", explode(col("tokens")).alias("token")) \
      .withWatermark("ts", args.watermark) \
      .groupBy(window("ts", args.window), "token") \
      .agg(count("token").alias("count")) \
      .select(
        col("window.start").alias("window_start"), 
        col("window.end").alias("window_end"), 
        col("token").alias("keyword"), 
        col("count").alias("appearances"), 
      ) \
      .writeStream \
      .foreachBatch(foreach_batch) \
      .option("checkpointLocation", checkpoint_location + "database-sink") \
      .queryName("keywords to database") \
      .start()

    tweetsToConsole = base_stream \
      .select("text") \
      .writeStream \
      .format("console") \
      .outputMode("append") \
      .option("truncate", "false") \
      .option("numRows", args.num_output_rows) \
      .option("checkpointLocation", checkpoint_location + "console") \
      .queryName("tweets to console") \
      .start()

    keywordsToDatabase.awaitTermination()
    tweetsToConsole.awaitTermination()
    spark.close()

if __name__ == '__main__':
    main()
