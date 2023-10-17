from google.cloud import storage

def gcs_read(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with blob.open("r") as f:
        print(f.read())

    return f


file_content = gcs_read("gyucegok-moodyspoc-test", "test.txt")

print(file_content)