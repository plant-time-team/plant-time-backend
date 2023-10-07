"""
plant parser
"""

import boto3
import csv
import io
import json

def lambda_handler(event, context):
    # Initialize clients
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    
    # Get the S3 object details from event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    # Fetch the CSV from S3
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    
    # Process the CSV
    table = dynamodb.Table('plants')
    for row in csv.DictReader(io.StringIO(content)):
        # Convert string representations of lists and dictionaries to actual list/dict
        for col in ['Insects', 'Origin', 'leaf_colour', 'Common_name']:
            row[col] = json.loads(row[col])

        for col in ['Height_potential', 'Width_potential', 'Temperature_max', 'Temperature_min', 'Pot_diameter']:
            row[col] = json.loads(row[col].replace("'", '"'))  # Ensure the quotes are in the right format

        # Insert into DynamoDB
        table.put_item(Item=row)
    
    return {
        'statusCode': 200,
        'body': json.dumps('CSV processed successfully!')
    }
