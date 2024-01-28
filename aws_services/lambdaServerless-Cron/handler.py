import json
import boto3


def hello(event, context):
    body = {
        "message": "Go Serverless v3.0! Your function executed successfully!",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}

def run(event, context):
    print("===== Hello Babai, Welcome to Serverless World.======")

    body = {
        "message": "Go Serverless v3.0! Your function executed successfully!",
        "input": event,
    }

    return {"statusCode":200, "body": json.dumps(body)}


def get_S3_data(event,context):

    s3 = boto3.client('s3')

    for records in event['Records']:
        bucketName = records['S3']['bucket']['name']
        objectKey  = records['S3']['bucket']['key']

        if records['eventName'] == 'ObjectCreated:Put':

            try:
                response = s3.get_object(Bucket=bucketName, Key=objectKey)
                objectData = response['Body'].read().decode('utf-8')

                outPut = '======= The Bucket Data is =======', objectData

                body = {
                    "message": "Go Serverless v3.0! Your function executed successfully!",
                    "input": event,
                    "Output": outPut
                }

                return {"statusCode": 200, "body": json.dumps(body)}
            except Exception as e:
                print(f"Error processing object: {e}")