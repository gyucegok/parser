import os
import time
from flask import Flask, request, Response
from google.cloud import storage
from google.cloud import bigquery

READ_BUCKET = "gyucegok-moodyspoc-test"
WRITE_BUCKET = "gyucegok-moodyspoc-test2"
BQ_TABLE_ID = "gyucegok-moodyspoc.statistics.parse-bucket-done"


def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        returntext = f.read()
#        print(returntext)
    return returntext


def gcs_write(bucket_name, blob_name, content):
    storage_client = storage.Client()

#    blob_name = blob_name + "." + str(time.time_ns())
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    
    with blob.open("w") as f:
        f.write(content)


def bq_stream_insert(filename, gcs_event_t, pubsub_publish_t, python_start_t, python_after_gcs_write_t):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    rows_to_insert = [
        {"filename": filename, "gcs_event_time": gcs_event_t, "pubsub_publish_time": pubsub_publish_t, "python_start_time": python_start_t, "python_after_gcs_write_time": python_after_gcs_write_t,},
    ]

    errors = client.insert_rows_json(BQ_TABLE_ID, rows_to_insert)  # Make an API request.
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))



app = Flask(__name__)

file_content = gcs_read(READ_BUCKET, "hello.txt")

@app.route('/', methods=['GET'])
def hello_world():
#    gcs_write(WRITE_BUCKET, "hello.txt", file_content)
    return file_content + "   Epoch: " + str(time.time_ns())

@app.route('/', methods=['POST'])
def gcs_notification():
    python_start_time = str(time.time_ns() // 1000000)
    jsondata = request.get_json()
    filename = jsondata['message']['attributes']['objectId']
    gcs_event_time = jsondata['message']['attributes']['eventTime']
    pubsub_publish_time = jsondata['message']['publishTime']
    filecontent = gcs_read(READ_BUCKET, filename)
    gcs_write(WRITE_BUCKET, filename, filecontent)
    python_after_gcs_write_time = str(time.time_ns() // 1000000)
    bq_stream_insert(filename, gcs_event_time, pubsub_publish_time, python_start_time, python_after_gcs_write_time)
    print(jsondata)
    print("Log for file: {} | gcs_event_time {} | pubsub_publish_time {} | python_start_time {} | python_after_gcs_write_time {}".format(filename,gcs_event_time, pubsub_publish_time,python_start_time,python_after_gcs_write_time))
#    print(jsondata)
#    print(jsondata['message'])
#    print(jsondata['message']['attributes']['objectId'])
#    print(jsondata['message']['data'])
    
    return request.json


if __name__ == "__main__":
    print(" Starting app...")
    app.run(host="0.0.0.0", port=8080)