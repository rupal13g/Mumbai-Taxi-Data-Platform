from google.cloud import storage

BUCKET_NAME = "mumbai-taxi-data-platform-rg1303"

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


def upload_json(data: str, object_name: str) -> str:
    blob = bucket.blob(object_name)

    blob.upload_from_string(
        data,
        content_type="application/json"
    )

    return f"gs://{BUCKET_NAME}/{object_name}"