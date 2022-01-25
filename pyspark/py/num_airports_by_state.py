import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, format_number

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file")
    parser.add_argument("--output_file")
    parser.add_argument("--output_format", default='json')
    parser.add_argument("--show_summary", type=bool, default=False)
    args = parser.parse_args()
    print('input_file={}, output_file={}, output_format={}, show_summary={}'\
        .format(args.input_file, args.output_file, args.output_format, args.show_summary))

    spark = SparkSession.builder.appName("num_airports_by_state_py").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    num_airports = spark.read.format("csv").option("header", "true").option("inferSchema","true") \
        .load(args.input_file) \
        .groupBy('STATE') \
        .agg(count(col('STATE')).alias('COUNT')) \
        .coalesce(1) \
        .sortWithinPartitions(['COUNT', 'STATE'], ascending=[False, True]) \

    num_airports.write.format(args.output_format).mode('overwrite') \
        .save(args.output_file)

    if(args.show_summary):
        num_airports.show()

    spark.stop()

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(f'\nElapsed time(sec): {end - start}')