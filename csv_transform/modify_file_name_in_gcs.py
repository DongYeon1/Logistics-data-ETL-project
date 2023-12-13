from google.cloud import storage


# Initialize the client
client = storage.Client()


# Get the bucket
bucket_name = "test-devcourse"
bucket = client.get_bucket(bucket_name)

for i in range(2, 313):
    old_file_name = f"2020/POS_POST ({i}).csv"
    new_file_name = f"2020/POS_POST_{i}.csv"

    # Get the blob (file) with the old name
    blob = bucket.blob(old_file_name)

    # Rename the blob with the new name
    new_blob = bucket.rename_blob(blob, new_name=new_file_name)

    print(f"File name modified from {old_file_name} to {new_file_name}")
