import argparse
import io
import requests
import os
import shutil
import pandas as pd
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

# Define the schema for the Parquet files
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

    """Function to create spark session and set logging level"""

    spark = SparkSession.builder.appName(session).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark session {session} created")
    return spark


def download_parquet_files(start_date, end_date, download_path):

    """Function that downloads the Parquet files of TLC trip record data
    and stores them in a dedicated path for further processing"""

    try:

        os.makedirs(download_path, exist_ok=True)

        # Delete files from other script executions
        shutil.rmtree(download_path)

        date_range = pd.date_range(start_date, end_date)
        download_urls = [
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date.strftime('%Y-%m')}.parquet"
            for date in date_range
        ]

        for url in download_urls:
            # Download the Parquet file from the URL
            response = requests.get(url)

            if response.status_code == 200:
                # Save in memory and write to the local file
                buffer_table = io.BytesIO(response.content)
                table = pq.read_table(buffer_table)
                local_file_path = os.path.join(download_path, os.path.basename(url))
                pq.write_table(table, local_file_path)
                print(f"Downloading {os.path.basename(url)}")
            else:
                print(f"Failed to download data from {url}")

    except Exception as e:
        print(f"Error: {str(e)}")


# Function to process Parquet data
def process_parquet_data(folder_path):
    sdf_list = []

    try:

        # Load the Parquet files from the local file system into a Spark DataFrame
        files = [f for f in os.listdir(folder_path) if f.endswith(".parquet")]

        for file_name in files:
            file_path = os.path.join(folder_path, file_name)
            # Read the Parquet file applying the custom schema
            sdf = spark.read.parquet(file_path)
            # Convert fields to our schema
            sdf = sdf.withColumn("VendorID", col("VendorID").cast("bigint"))
            # Add the DataFrame to the list
            sdf_list.append(sdf)

        # Append the DataFrames in the list
        sdf = sdf_list[0]
        print("Appending parquet files")
        for sdf in sdf_list[1:]:
            sdf = sdf.union(sdf)

        sdf = sdf.orderBy(col("trip_distance").desc())

        # Calculate 10% of the total and reassign the initial sdf
        top_10per_rows = int(final_sdf.count() * 0.10)
        sdf = sdf.limit(top_10per_rows)

        return sdf

    except Exception as e:
        print(f"Error processing parquet data: {str(e)}")
        return None


def data_quality_total_amount(sdf):

    """ Check and correct data quality issues in the input DataFrame """

    try:

        # Check for null values in the "total_amount" column
        null_check = sdf.filter(col("total_amount").isNull())

        # Check for negative values in "total_amount" and correct other columns accordingly
        negative_check = sdf.filter(col("total_amount") < 0)

        if not null_check.isEmpty():
            print("Null values found in the 'total_amount' column.")

        if not negative_check.isEmpty():
            print("Negative 'total_amount' values found. Correcting...")

            # Define a list of columns to correct
            columns_to_correct = [
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount"
            ]

            # Multiply selected columns by -1 for rows with negative "total_amount"
            for column in columns_to_correct:
                sdf = sdf.withColumn(column, when(col("total_amount") < 0, col(column) * -1).otherwise(col(column)))

        # Check data type of "total_amount" column
        if sdf.schema["total_amount"].dataType != DoubleType():
            print("The data type of 'total_amount' column is not DoubleType.")

        return null_check, negative_check, sdf

    except Exception as e:

        print(f"Error in data quality check: {str(e)}")
        return None, None, None


# Function to parse command line arguments
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

    processed_data = process_parquet_data(args.download_path)

    null_check, negative_check, final_sdf = data_quality_total_amount(processed_data)

    final_sdf.write.mode("overwrite").parquet('processed_data')

    null_check.write.mode("overwrite").parquet('processed_data/null_data')

    negative_check.write.mode("overwrite").parquet('processed_data/negative_amount')

    spark.stop()
