# handlers.py
from registry import register
import boto3
from pyspark.sql import SparkSession

import boto3
import json

# Decorator to register this function, assuming some framework or custom decorator
@register('sqs')
def handle_sqs(parameters):
    # Initialize SQS client
    sqs = boto3.client('sqs')
    
    # Get parameters
    queue_url = parameters['queue_url']
    wait_time_seconds = parameters.get('wait_time_seconds', 0)
    
    # Receive a message from the SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,  # Adjust the number of messages to receive as needed
        WaitTimeSeconds=wait_time_seconds
    )
    
    # Extract messages from the response
    messages = response.get('Messages', [])
    
    if messages:  # Check if any messages were received
        file_names = []
        
        for message in messages:
            body = json.loads(message['Body'])  # Parse the message body as JSON
            
            # Assume the SQS message contains S3 event notification format
            records = body.get('Records', [])
            for record in records:
                s3_info = record.get('s3', {})
                bucket_name = s3_info.get('bucket', {}).get('name')
                file_name = s3_info.get('object', {}).get('key')
                
                if bucket_name and file_name:
                    file_names.append(file_name)
            
            # After processing the message, delete it from the SQS queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        
        return file_names
    
    else:
        print("No messages received")
        return []



@register('s3')
def handle_s3(parameters, dataframe=None):
    operation = parameters.get('operation', 'default')
    if operation == 'pull':
        bucket_name = parameters['bucket_name']
        key_prefix = parameters['file_name']
        input_path = f"s3://{bucket_name}/{key_prefix}/"
        spark = SparkSession.builder.getOrCreate()
        dataframe = spark.read.csv(input_path)
        print(f"Data successfully pulled from {input_path}")
        return dataframe
    elif operation == 'load':
        bucket_name = parameters['bucket_name']
        output_path = f"s3://{bucket_name}/config/"
        dataframe.coalesce(1).write.mode("overwrite").parquet(output_path)
        print(f"Data successfully loaded to {output_path}")

@register('sql')
def handle_sql_transform(parameters, dataframe):
    query = parameters['query']
    dataframe.createOrReplaceTempView("temp_table")
    spark = SparkSession.builder.getOrCreate()
    transformed_df = spark.sql(query)
    print("SQL transformation completed.")
    return transformed_df
