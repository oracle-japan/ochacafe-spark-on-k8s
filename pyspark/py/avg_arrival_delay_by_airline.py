import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, format_number

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir")
    parser.add_argument("--output_dir")
    parser.add_argument("--output_format", default='json')
    parser.add_argument("--show_summary", type=bool, default=False)
    args = parser.parse_args()
    print('input_dir={}, output_dir={}, output_format={}, show_summary={}'\
        .format(args.input_dir, args.output_dir, args.output_format, args.show_summary))

    spark = SparkSession.builder.appName("avg_delay_by_airline_py").getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    airlines = spark.read.format("csv").option("header", "true").option("inferSchema","true") \
        .load(f"{args.input_dir}/airlines.csv") \
        .select('IATA_CODE', col('AIRLINE').alias('AIRLINE_NAME'))

    flights = spark.read.format("csv").option("header", "true").option("inferSchema","true") \
        .load(f"{args.input_dir}/flights.csv")

    avg_arrival_delay = flights.groupBy('AIRLINE') \
        .agg(count(col('AIRLINE')).alias('FLIGHTS_COUNT'), avg(col('ARRIVAL_DELAY')).alias('AVG_ARRIVAL_DELAY')) \
        .withColumn('AVG_ARRIVAL_DELAY_BY_MINUTES', format_number(col('AVG_ARRIVAL_DELAY'), 2)) \
        .join(airlines, flights.AIRLINE == airlines.IATA_CODE, 'left') \
        .coalesce(1) \
        .sortWithinPartitions('AVG_ARRIVAL_DELAY') \
        .select(col('AIRLINE_NAME'), col('FLIGHTS_COUNT'), col('AVG_ARRIVAL_DELAY_BY_MINUTES')) \

    avg_arrival_delay.write.format(args.output_format).mode('overwrite') \
        .save(f'{args.output_dir}/avg_arrival_delay_by_airline')

    if(args.show_summary):
        num_flights = 0
        for row in avg_arrival_delay.collect():
            num_flights += row['FLIGHTS_COUNT']
            print(f"{row['AIRLINE_NAME']:32} {row['FLIGHTS_COUNT']:10d} {row['AVG_ARRIVAL_DELAY_BY_MINUTES']:>8}")
        print(f'Total {num_flights:,} flights.')

    spark.stop()

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(f'\nElapsed time(sec): {end - start}')