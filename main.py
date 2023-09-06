import argparse
import io
import requests
import os
import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType, IntegerType

schema = StructType([
    StructField("VendorID", LongType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", DoubleType()),
    StructField("trip_distance", DoubleType()),
    StructField("RatecodeID", DoubleType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", LongType()),
    StructField("DOLocationID", LongType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("congestion_surcharge", DoubleType()),
    StructField("airport_fee", DoubleType())
])

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

            # Guarda en memoria y escribe a la local

                buffer_table = io.BytesIO(response.content)
                table = pq.read_table(buffer_table)

                local_file_path = os.path.join(download_path, os.path.basename(url))

                pq.write_table(table, local_file_path)

            else:
                print(f"Failed to download data from {url}")

    except Exception as e:
        print(f"Error: {str(e)}")


def process_parquet_data(folder_path):

    sdf_list = []

    # Cargamos los archivos parquet desde el sistema de archivos local a un dataframe de spark

    files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]

    for file_name in files:
        file_path = os.path.join(folder_path, file_name)

        # Lee el archivo Parquet aplicando el esquema personalizado
        sdf = spark.read.parquet(file_path)

        # Convertimos campos a nuestro schema

        sdf = sdf.withColumn("VendorID", col("VendorID").cast("bigint"))

        # Agrega el DataFrame a la lista
        sdf_list.append(sdf)

    # Realiza un append de los DataFrames en la lista
    final_sdf = sdf_list[0]

    for sdf in sdf_list[1:]:
        final_sdf = final_sdf.union(sdf)

    final_sdf = final_sdf.orderBy(col("trip_distance").desc())

    # Calculamos el 10% del total y reasignamos el sdf inicial

    top_10per_rows = int(final_sdf.count() * 0.10)
    final_sdf = final_sdf.limit(top_10per_rows)

    return final_sdf

def parser():

    """Function to parse arguments on the script"""

    parser = argparse.ArgumentParser(description="Download and process Yellow Taxi trip data.")
    parser.add_argument("start_date", help="Start date in yyyy-MM format")
    parser.add_argument("end_date", help="End date in yyyy-MM format")
    parser.add_argument("--download_path", default="raw_data", help="Download path for Parquet files")

    return parser.parse_args()

if __name__ == '__main__':

    spark = create_spark_session("TripRecordData")

    args = parser()

    download_parquet_files(args.start_date, args.end_date, args.download_path)

    process_parquet_data(args.download_path)

    spark.stop()
