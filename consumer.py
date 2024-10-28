import time
import argparse
import json
import boto3
import re
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
dynamodb_client = boto3.resource('dynamodb')

parser = argparse.ArgumentParser(description="Widget request command.")
parser.add_argument("--storage", choices=["s3", "dynamodb"], required=True, help="storage choice")
parser.add_argument("--bucket", help="S3 bucket name")
parser.add_argument("--table", help="DynamoDB table name")
parser.add_argument("--interval", type=int, default=100, help="poll interval")
args = parser.parse_args()

if args.storage == 's3' and not args.bucket:
    parser.error("S3 needs a bucket")
elif args.storage == 'dynamodb' and not args.table:
    parser.error("DynamoDB need a table")

table = dynamodb_client.Table(args.table) if args.storage == 'dynamodb' else None

def get_widget_request(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        
        if 'Contents' in response and len(response['Contents']) > 0:
            key = response['Contents'][0]['Key']
            req_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            req_data = json.loads(req_obj['Body'].read().decode('utf-8'))
            return req_data, key
        else:
            logging.info(f"No objects in bucket '{bucket_name}'.")
            return None, None
    except ClientError as error_message:
        print(f"Failed to get request in S3: {error_message}")
        return None, None
                
# Used to debug, ensures schema has everything that is needed.
def check_schema(req_data):
    req_fields = {"type", "requestId", "widgetId", "owner"}
    if not all(field in req_data for field in req_fields):
        raise ValueError("missing fields in request")
    if not re.match(r"create|delete|update", req_data["type"]):
        raise ValueError("request was not 'create', 'delete', or 'update'")

def s3_store(widget_data, bucket_name):
    owner_path = widget_data["owner"].replace(" ", "-").lower()
    widget_id = widget_data["widgetId"]
    key = f"widgets/{owner_path}/{widget_id}.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(widget_data)
        )
        print(f"widget {widget_id} stored in {key}")
    except ClientError as error_message:
        print(f"unable to store widget: {error_message}")

def dynamodb_store(widget_data):
    try:
        item = {
            "id": widget_data["widgetId"],
            "owner": widget_data["owner"],
            "label": widget_data.get("label", ""),
            "description": widget_data.get("description", "")
        }
        for attribute in widget_data.get("otherAttributes", []):
            item[attribute["name"]] = attribute["value"]
        table.put_item(Item=item)
        print(f"widget {widget_data['widgetId']} stored")
    except ClientError as error_message:
        print(f"unable to store widget: {error_message}")

def create_request_handle(widget_data):
    if args.storage == "s3":
        s3_store(widget_data, args.bucket)
    elif args.storage == "dynamodb":
        dynamodb_store(widget_data)

def execute_request(req_data):
    if req_data["type"] == "create":
        create_request_handle(req_data)
    elif req_data["type"] == "delete":
        print("Delete request implementation PLACEHOLDER")
    elif req_data["type"] == "update":
        print("Update request implementation PLACEHOLDER")
    else:
        print(f"Request not recognized: {req_data['type']}")

def main():
    while True:
        req_data, key = get_widget_request("usu-cs5250-matrix2507-requests")
        if req_data:
            try:
                check_schema(req_data)
                execute_request(req_data)
                s3_client.delete_object(Bucket="usu-cs5250-matrix2507-requests", Key=key)
            except ValueError as error_message:
                print(f"Schema validation error: {error_message}")
            except json.JSONDecodeError:
                print("JSON not formatted correctly")
        else:
            time.sleep(args.interval / 1000)

if __name__ == "__main__":
    main()
