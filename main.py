import os
import time
from flask import Flask, Response
from google.cloud import storage
# from google.cloud import pubsub_v1

def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        returntext = f.read()
        print(returntext)
    return returntext

from google.cloud import storage


def gcs_write(bucket_name, blob_name, content):
    storage_client = storage.Client()

    blob_name = blob_name + "." + str(time.time_ns())
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    
    with blob.open("w") as f:
        f.write(content)



file_content = gcs_read("gyucegok-moodyspoc-test", "hello.txt")

app = Flask(__name__)

@app.route('/')
def hello_world():
    gcs_write("gyucegok-moodyspoc-test2", "hello.txt", file_content)
    return file_content + "   Epoch: " + str(time.time_ns())

if __name__ == "__main__":
    print(" Starting app...")
    app.run(host="0.0.0.0", port=8080)