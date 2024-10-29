import argparse
import boto3

S3 = boto3.client('s3')

def create_request(widget_details, storage):
        widget_id = widget_request.get('widget_id')
        owner = widget_request.get('owner')
        widget_data = json.dumps(widget_request)

        if args.store == 's3':
            s3_key = f"widgets/{owner.replace(' ', '-').lower()}/{widget_id}"
            self.s3.put_object(Bucket=self.bucket_output, Key=s3_key, Body=widget_data)
            logging.info(f"Stored widget {widget_id} in S3 bucket {self.bucket_output} under key {s3_key}")
        elif args.store == 'dynamodb':
            self.store_in_dynamodb(widget_request)
            logging.info(f"Stored widget {widget_id} in DynamoDB table {self.dynamodb_table}")
        
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
    parser.add_argument('--s3', required=False, help="Specify storage target: s3")
    parser.add_argument('--dynamodb', required=False, help="Specify storage target: dynamodb")
    args = parser.parse_args()
    print(args)
    run(args.storage, args.input)

if __name__ == "__main__":
    main()
