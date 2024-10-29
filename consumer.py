import argparse

def create_request(widget_details, storage):
    if storage == 's3':
        # add to s3 bucket
        print(storage)
    elif storage == 'dynamodb':
        # add to dynamo db
        print(storage)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage', choices=['s3', 'dynamodb'], required=True, help="Specify storage target: s3 or dynamodb")
    args = parser.parse_args()
    create_request("hello", args.storage)
    print(args.storage)

if __name__ == "__main__":
    main()

