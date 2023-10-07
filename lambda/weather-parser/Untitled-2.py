"""
weather parser
"""

import json
import requests
import boto3
from datetime import datetime, timedelta

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourDynamoDBTableName')

def lambda_handler(event, context):
    # Extract parameters from the event
    latitude = event['queryStringParameters']['latitude']
    longitude = event['queryStringParameters']['longitude']
    location = f"{latitude}_{longitude}"  # e.g., "52.52_13.419998"

    try:
        response = requests.get(f"https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&daily=weathercode,temperature_2m_max,temperature_2m_min,uv_index_max,precipitation_sum&timezone=America%2FNew_York&start_date=2022-09-30&end_date=2023-10-05")
        response.raise_for_status()
        data = response.json()

        for i in range(len(data['daily']['time'])):
            date = data['daily']['time'][i]
            weathercode = data['daily']['weathercode'][i] or 0
            temperature_max = data['daily']['temperature_2m_max'][i] or 0.0
            temperature_min = data['daily']['temperature_2m_min'][i] or 0.0
            uv_index_max = data['daily']['uv_index_max'][i] or 0.0
            precipitation_sum = data['daily']['precipitation_sum'][i] or 0.0

            # Insert into DynamoDB
            table.put_item(
                Item={
                    'location': location,
                    'date': date,
                    'weathercode': weathercode,
                    'temperature_max': Decimal(str(temperature_max)),  # DynamoDB requires numbers to be of type Decimal
                    'temperature_min': Decimal(str(temperature_min)),
                    'uv_index_max': Decimal(str(uv_index_max)),
                    'precipitation_sum': Decimal(str(precipitation_sum))
                }
            )

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({"message": "An error occurred.", "error": str(e)})
        }

    return {
        'statusCode': 200,
        'body': json.dumps({"message": "Data processed and stored in DynamoDB"}),
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }