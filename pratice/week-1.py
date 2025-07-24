import google.cloud.storage as storage
storage_client = storage.Client()
bucket = storage_client.bucket("intro-to-big-data-week-1")
blob = bucket.blob("Week-2-test-file.txt")
print(len(blob.download_as_text().split()))