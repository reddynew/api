# handlers.py
from registry import register
import boto3
from pyspark.sql import SparkSession

@register('sqs')
def handle_sqs(parameters):
    sqs = boto3.client('sqs')
    queue_url = parameters['queue_url']
    wait_time_seconds = parameters.get('wait_time_seconds', 0)
    
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=wait_time_seconds
    )
    
    messages = response.get('Messages', [])
    if messages:
        print(f"Received message: {messages[0]['Body']}")
        # Process the message here...
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=messages[0]['ReceiptHandle'])
    else:
        print("No messages received")

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
