# trip-record-data

This project focuses on downloading and processing New York City yellow taxi trip data in Parquet format using Apache Spark and Python. The data is downloaded from an external source, processed, and data quality checks are performed before saving the results in a processed format.

In this case, the decision has been made to first download all the raw files and then append them together to create a larger file for further processing (counting, ordering, filtering, etc.) with PySpark. Optimized processing, better parallelism and reduced metatada are some of the advantages of working with a single file. Also parquet format is optimized for larger datasets by using columnar storage.

## Requirements

- Python 3.x
- Apache Spark (pyspark)
- Python libraries: pandas, pyarrow, requests

## Usage

1. Clone the repository to your local machine.

   ```bash
   git clone https://github.com/your-username/your-repository.git
   cd your-repository

2. Ensure that the mentioned requirements in `requirements.txt` are installed in your environment.

3. Run the main script main.py, providing the start and end dates as well as an optional download location for the downloaded Parquet files.

   ```bash
   python main.py 2023-01 2023-02 --download_path raw_data

4. This will download New York City yellow taxi trip data for the specified period in Parquet format.

The downloaded data will be processed, and data quality checks will be performed. 
These checks are based on whether the 'total_amount' column has null data, values less than zero, or if the data type is incorrect.
The processed results will be saved in the processed_data folder. Null data will be saved in processed_data/null_data, and data with negative amounts will be saved in processed_data/negative_amount.

Error logs and progress will be displayed in the console.

## Project Structure

- `main.py`: Main script for downloading, processing, and data quality checks.
- `README.md`: This file.
- `raw_data/`: Default folder containing downloaded parquet files.
- `processed_data/`: Folder containing processed data.
  - `null_data/`: Folder containing data with null values.
  - `negative_amount/`: Folder containing data with negative amounts.
