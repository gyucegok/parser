import os
import time
from flask import Flask, request, Response
from google.cloud import storage


def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        returntext = f.read()
        print(returntext)
    return returntext


def gcs_write(bucket_name, blob_name, content):
    storage_client = storage.Client()

    blob_name = blob_name + "." + str(time.time_ns())
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    
    with blob.open("w") as f:
        f.write(content)


app = Flask(__name__)

file_content = gcs_read("gyucegok-moodyspoc-test", "hello.txt")

@app.route('/', methods=['GET'])
def hello_world():
    gcs_write("gyucegok-moodyspoc-test2", "hello.txt", file_content)
    return file_content + "   Epoch: " + str(time.time_ns())

@app.route('/', methods=['POST'])
def gcs_notification():
    jsondata = request.get_json()
    print(jsondata)
#    print(jsondata['message'])
#    print(jsondata['message']['attributes']['objectId'])
#    print(jsondata['message']['data'])
    return request.json


if __name__ == "__main__":
    print(" Starting app...")
    app.run(host="0.0.0.0", port=8080)