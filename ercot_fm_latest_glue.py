import sys
import boto3
import pandas as pd
import datetime
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

# Developer: Your Name
# Purpose: This script fetches ERCOT fuel mix data, transforms it, and stores it in S3,
#          while registering it in the Glue Data Catalog.

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
logger = glueContext.get_logger()

def fetch_fuel_mix_data():
    # Simulated fetch function (replace this with your actual data fetching logic)
    # For example, using requests to fetch from an API
    # response = requests.get("YOUR_API_URL")
    # data = response.json()
    # For now, we'll create a dummy DataFrame
    data = {
        "fuel_type": ["Natural Gas", "Coal", "Wind", "Solar"],
        "percentage": [55, 25, 15, 5],
        "timestamp": [datetime.datetime.now()] * 4
    }
    df = pd.DataFrame(data)
    return df

def upload_to_s3_and_register_catalog(df, bucket_name, folder_name, table_name):
    timestamp = datetime.datetime.now()
    filename = f"ercot_fm_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    s3_path = f"s3://{bucket_name}/{folder_name}/{filename}"

    # Convert DataFrame to CSV and upload to S3
    csv_buffer = df.to_csv(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=bucket_name, Key=f"{folder_name}/{filename}", Body=csv_buffer)
    logger.info(f"File {filename} uploaded successfully to S3 bucket {bucket_name}.")

    # Register the table in Glue Data Catalog
    glue_client = boto3.client('glue')
    response = glue_client.create_table(
        DatabaseName='ercot_database',  # Replace with your database name
        TableInput={
            'Name': table_name,
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'fuel_type', 'Type': 'string'},
                    {'Name': 'percentage', 'Type': 'double'},
                    {'Name': 'timestamp', 'Type': 'timestamp'}
                ],
                'Location': f"s3://{bucket_name}/{folder_name}/",
                'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                'Compressed': False,
                'SerdeInfo': {
                    'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde,
                    'Parameters': {
                        'separatorChar': ',',
                        'quoteChar': '"',
                    }
                },
            },
            'TableType': 'EXTERNAL_TABLE',
        }
    )
    logger.info(f"Table {table_name} registered in Glue Data Catalog.")

def main():
    bucket_name = "ercot-test"  # Replace with your bucket name
    folder_name = "ercot_fm_csv/fm_latest"
    table_name = "ercot_fuel_mix"  # Name of the table in Glue Data Catalog

    try:
        df = fetch_fuel_mix_data()
        upload_to_s3_and_register_catalog(df, bucket_name, folder_name, table_name)
        logger.info(f"Data processing completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
