import argparse
import boto3

def create_request(widget_details, storage):
    if 's3' in storage:
        s3 = boto3.client('s3')
        print(f"Adding to S3 bucket: {widget_details}")
    elif storage == 'dynamodb':
        print(f"Adding to DynamoDB: {widget_details}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Specify a bucket to read widget requests from")
    parser.add_argument('--storage', choices=['s3', 'dynamodb'], required=True, help="Specify storage target: s3 or dynamodb")
    args = parser.parse_args()
    create_request(args.input, args.storage)
    print(f"Storage target: {args.storage}")

if __name__ == "__main__":
    main()