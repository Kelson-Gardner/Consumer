This program will allow you to take widget objects that are being created by a producer.jar file and place them into either a dynamodb database or an s3 bucket.

To run the program run this command:

python3 consumer.py --input [bucket_input_name] [--s3 [name of s3 bucket to write to]] || [--dynamodb [name of dynamodb to write to]]

You can use either of the options --s3 or --dynamodb. Whichever argument you enter will determine where you put the widgets.
