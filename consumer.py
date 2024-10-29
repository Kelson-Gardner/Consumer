import argparse
import boto3

S3 = boto3.client('s3')

def create_request(widget_details, storage):
    if 's3' in storage:
        s3 = boto3.client('s3')
        print(f"Adding to S3 bucket: {widget_details}")
    elif storage == 'dynamodb':
        print(f"Adding to DynamoDB: {widget_details}")
        
def get_requests(input_bucket):
    response = S3.list_objects_v2(Bucket=input_bucket)
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            print(key)
            return key
    return None

def run(storage, input_bucket):
    """Run the Consumer to process Widget Requests."""
    while True:
        widget_request = get_requests(input_bucket)
        if widget_request:
            create_request(widget_request, storage)
        else:
            time.sleep(0.1)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, help="Specify a bucket to read widget requests from")
    parser.add_argument('--storage', choices=['s3', 'dynamodb'], required=True, help="Specify storage target: s3 or dynamodb")
    args = parser.parse_args()
    print(args)
    run(args.storage, args.input)

if __name__ == "__main__":
    main()
