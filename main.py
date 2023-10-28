import os
import time
import datetime
import re
from flask import Flask, request, Response
from google.cloud import storage
from google.cloud import bigquery

READ_BUCKET = os.environ['READ_BUCKET']
WRITE_BUCKET = os.environ['WRITE_BUCKET']
BQ_TABLE_ID = os.environ['BQ_TABLE_ID']


def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    try:
        with blob.open("r") as f:
            returntext = f.read()
            #print(returntext)
        return returntext
    except Exception as e:
        print("File {} not available | Error/Exception: {}".format(blob_name, e))
        return -1


def gcs_write(bucket_name, blob_name, content):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
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

def convert_timestring_to_epoch(time_str, who):
    if re.match("%Y-%m-%dT%H:%M:%S.%fZ", time_str):
        time_obj = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        epoch_time_in_ms = round(int(time_obj.timestamp() * 1000))
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, epoch_time_in_ms))
        return epoch_time_in_ms
    elif re.match("%Y-%m-%dT%H:%M:%SZ", time_str):
        time_obj = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
        epoch_time_in_ms = round(int(time_obj.timestamp() * 1000))
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, epoch_time_in_ms))
        return epoch_time_in_ms
    else:
        print("{} Time STR: {} | Epoch: {}".format(who,time_str, "-1"))
        return -1


app = Flask(__name__)

@app.route('/', methods=['GET'])
def hello_world():
#    gcs_write(WRITE_BUCKET, "hello.txt", file_content)
#    return file_content + "   Epoch: " + str(time.time_ns())
    return "Hello World" + "   Epoch: " + str(time.time_ns())

@app.route('/', methods=['POST'])
def gcs_notification():
    python_start_time = str(time.time_ns() // 1000000)
    jsondata = request.get_json()
    filename = jsondata['message']['attributes']['objectId']
    filecontent = gcs_read(READ_BUCKET, filename)
    if filecontent == -1:
        return '', 204
    gcs_write(WRITE_BUCKET, filename, filecontent)
    python_after_gcs_write_time = str(time.time_ns() // 1000000)
    gcs_event_time = convert_timestring_to_epoch(jsondata['message']['attributes']['eventTime'], "Event Time")
    pubsub_publish_time = convert_timestring_to_epoch(jsondata['message']['publishTime'], "Publish Time")
    bq_stream_insert(filename, gcs_event_time, pubsub_publish_time, python_start_time, python_after_gcs_write_time)
    print(jsondata)
    print("Log for file: {} | gcs_event_time {} | pubsub_publish_time {} | python_start_time {} | python_after_gcs_write_time {}".format(filename,gcs_event_time, pubsub_publish_time,python_start_time,python_after_gcs_write_time))
#    print(jsondata)
#    print(jsondata['message'])
#    print(jsondata['message']['attributes']['objectId'])
#    print(jsondata['message']['data'])
    return '', 204

if __name__ == "__main__":
    print(" Starting app...")
    app.run(host="0.0.0.0", port=8080)