import argparse
import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType


def create_spark_session(session):
    spark = SparkSession.builder.appName(session).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark session {session} created")
    return spark


def download_parquet_files(start_date, end_date, download_path):

    """Function that downloads the parquet file on TLC trip record data and store them in dedicated path to further
    processing"""

    try:

        os.makedirs(download_path, exist_ok=True)

        date_range = pd.date_range(start_date, end_date)
        download_urls = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date.strftime('%Y-%m')}.parquet"
            for date in date_range
        ]

        for url in download_urls:

            # Descarga el archivo Parquet desde la URL

            response = requests.get(url)

            if response.status_code == 200:

                local_file_path = os.path.join(download_path, os.path.basename(url))

                with open(local_file_path, 'wb') as local_file:
                    local_file.write(response.content)

            else:
                print(f"Failed to download data from {url}")

    except Exception as e:
        print(f"Error: {str(e)}")


def process_parquet_data(path):

    # Cargamos los archivos parquet desde el sistema de archivos local a un dataframe de spark

    sdf = spark.read \
           .option("header", "true") \
           .parquet(path) \
           .orderBy("trip_distance")

    # Calculamos el 10% del total y reasignamos el sdf inicial

    top_10_rows = int(sdf.count() * 0.10)
    sdf = sdf.limit(top_10_rows)

    return sdf.count()


if __name__ == '__main__':

    spark = create_spark_session("TripRecordData")
    # parser = argparse.ArgumentParser(description="Download and process Yellow Taxi trip data.")
    # parser.add_argument("start_date", help="Start date in yyyy-MM format")
    # parser.add_argument("end_date", help="End date in yyyy-MM format")
    # parser.add_argument("--download_path", default="yellow_taxi_data", help="Download path for Parquet files")
    #
    # args = parser.parse_args()
    #
    # download_parquet_files(args.start_date, args.end_date, args.download_path)
    # download_parquet_files('2023-02', '2023-02',
    #                        """/Users/eloygomez-caromoreno/PycharmProjects/trip-record-data/raw_data""")

    # process_parquet_data(args.download_path)
    process_parquet_data('/Users/eloygomez-caromoreno/PycharmProjects/trip-record-data/raw_data')

    spark.stop()
