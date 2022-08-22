import boto3
from pprint import pprint
import os

class S3LOANLOAD:
    _s3Cred = boto3.resource(
        "s3",
        aws_access_key_id=os.environ['aws_access_key_id'],
        aws_secret_access_key=os.environ['aws_secret_access_key'],
    )

    def __init__(self):
        self.s3 = S3LOANLOAD._s3Cred

    # load csv file to s3 bucket
    def load_to_s3(self, transformedDF):
        obj = self.s3.Object(
            bucket_name="lti871", key="prashant/output_historicalData.csv"
        )
        obj.put(Body=open("data/output.csv", "rb"))

    # Pipelining previous functions

    def run(self, transformedDF):
        self.load_to_s3(transformedDF)
