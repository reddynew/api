# glue_job.py

import boto3
import yaml
import importlib.util
import sys

# Function to load YAML configuration from S3
def load_config_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    config = yaml.safe_load(obj['Body'].read())
    return config

# Function to dynamically import a Python module from S3
def import_module_from_s3(bucket_name, key, module_name):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    script = obj['Body'].read().decode('utf-8')
    
    spec = importlib.util.spec_from_loader(module_name, loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(script, module.__dict__)
    sys.modules[module_name] = module

# Load modules
import_module_from_s3('templatedemobucket', 'scripts/registry.py', 'registry')
import_module_from_s3('templatedemobucket', 'scripts/handlers.py', 'handlers')

from registry import retrieve
import handlers

# Function to execute steps in the pipeline dynamically
def run_step(step_type, parameters, dataframe=None):
    handler = retrieve(step_type)
    return handler(parameters, dataframe)

# Main Glue job logic
def main():
    # Load pipeline configuration from S3
    config = load_config_from_s3('templatedemobucket', 'config.yml')
    
    dataframe = None
    for step in config['steps']:
        print(f"Running step: {step['name']}")
        dataframe = run_step(step['type'], step['parameters'], dataframe)

# Entry point for Glue job
if __name__ == "__main__":
    main()
