import argparse
import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, format_number

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file")
    parser.add_argument("--show_summary", type=bool, default=False)
    args = parser.parse_args()
    print('input_file={}, show_summary={}'\
        .format(args.input_file, args.show_summary))

    spark = SparkSession.builder.appName("num_airports_to_db_py").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    num_airports = spark.read.format("csv").option("header", "true").option("inferSchema","true") \
        .load(args.input_file) \
        .groupBy('STATE') \
        .agg(count(col('STATE')).alias('COUNT')) \
        .coalesce(1) \
        .sortWithinPartitions(['COUNT', 'STATE'], ascending=[False, True]) \

    db_prop = {
        'user' : os.environ['DB_USER'], 
        'password' : os.environ['DB_PASSWORD'], 
        'driver' : "oracle.jdbc.driver.OracleDriver" 
    }
    num_airports.write.option('truncate', 'true') \
        .jdbc(os.environ['DB_JDBCURL'], "NUM_AIRPORTS", "overwrite", properties=db_prop)

    if(args.show_summary):
        num_airports.show()

    spark.stop()

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(f'\nElapsed time(sec): {end - start}')