import os
from flask import Flask
from google.cloud import storage
# from google.cloud import pubsub_v1

def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())

    return f

app = Flask(__name__)

file_content = gcs_read("gyucegok-moodyspoc-test", "hello.txt")

@app.route('/')
def hello_world():
    return file_content