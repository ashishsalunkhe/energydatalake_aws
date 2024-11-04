import sys
import boto3
import pandas as pd
import datetime
import logging
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Developer: Shashank
# Purpose: This script fetches ERCOT load forecast data, transforms it, and stores it in S3,
#          while registering it in the Glue Data Catalog.

# Set up logging
logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

def fetch_load_forecast_data():
    # Simulated fetch function (replace this with your actual data fetching logic)
    # For example, using requests to fetch from an API
    # response = requests.get("YOUR_API_URL")
    # data = response.json()
    # For now, we'll create a dummy DataFrame
    data = {
        "forecast_time": [datetime.datetime.now()],
        "load_mw": [50000],  # Example load in megawatts
        "temperature": [85]   # Example temperature in degrees Fahrenheit
    }
    df = pd.DataFrame(data)
    return df

def upload_to_s3_and_register_catalog(df, bucket_name, folder_name, table_name):
    timestamp = datetime.datetime.now()
    filename = f"ercot_load_forecast_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    s3_path = f"s3://{bucket_name}/{folder_name}/{filename}"

    # Convert DataFrame to CSV and upload to S3
    csv_buffer = df.to_csv(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=f"{folder_name}/{filename}", Body=csv_buffer)
    logging.info(f"File {filename} uploaded successfully to S3 bucket {bucket_name}.")

    # Register the table in Glue Data Catalog
    glue_client = boto3.client('glue')
    response = glue_client.create_table(
        DatabaseName='ercot_database',  # Replace with your database name
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'forecast_time', 'Type': 'timestamp'},
                    {'Name': 'load_mw', 'Type': 'double'},
                    {'Name': 'temperature', 'Type': 'double'}
                ],
                'Location': f"s3://{bucket_name}/{folder_name}/",
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Compressed': False,
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde',
                    'Parameters': {
                        'separatorChar': ',',
                        'quoteChar': '"',
                    }
                },
            },
            'TableType': 'EXTERNAL_TABLE',
        }
    )
    logging.info(f"Table {table_name} registered in Glue Data Catalog.")

def main():
    bucket_name = "ercot-test"  # Replace with your bucket name
    folder_name = "ercot_load_forecast_csv"
    table_name = "ercot_load_forecast"  # Name of the table in Glue Data Catalog

    try:
        df = fetch_load_forecast_data()
        upload_to_s3_and_register_catalog(df, bucket_name, folder_name, table_name)
        logging.info("Data processing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
