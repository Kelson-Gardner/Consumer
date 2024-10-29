import argparse

def create_request(widget_details, storage):
    if 'bucket' in storage:
        # add to s3 bucket
        print(storage)
    elif storage == 'dynamodb':
        # add to dynamo db
        print(storage)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage', choices=['bucket2', 'bucket3' 'dynamodb'], required=True, help="Specify storage target: s3 (bucket2 or bucket3) or dynamodb")
    args = parser.parse_args()
    create_request("hello", args.storage)
    print(args.storage)

if __name__ == "__main__":
    main()

