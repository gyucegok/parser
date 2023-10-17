import os
from flask import Flask, Response
# from google.cloud import storage
# from google.cloud import pubsub_v1

""" def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())

    return f

file_content = gcs_read("gyucegok-moodyspoc-test", "hello.txt") """


app = Flask(__name__)

@app.route('/')
def hello_world():
 #   return file_content
    return f"hello!!!"

if __name__ == "__main__":
    print(" Starting app...")
    app.run(host="0.0.0.0", port=8080)