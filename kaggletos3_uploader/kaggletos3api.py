import os
import boto3
from zipfile import ZipFile
import json

from kaggle.api.kaggle_api_extended import KaggleApi
from credentials import Credentials


setting = Credentials()

kaggle_username = setting.kaggle_username
kaggle_key = setting.kaggle_key
s3_bucket_name = setting.s3_bucket_name
s3_directory = setting.s3_directory_name

api = KaggleApi()
api.authenticate()
s3 = boto3.client('s3')

# Specify the datasets you want to download from Kaggle (these are example)
datasets = [
    'rtatman/digidb',
    'juniorbueno/opencv-facial-recognition-lbph'
    # 'kmader/skin-cancer-mnist-ham10000'

]

# Specify the destination folder for downloading and extracting the ZIP file
destination_folder = "./datasets" 
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

# Specify the checkpoint file to keep track of uploaded files
checkpoint_file = "upload_checkpoint.json"

uploaded_files = []  # List to keep track of uploaded files

# Check if there is a checkpoint file to resume from
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, 'r') as f:
        uploaded_files = json.load(f)

for dataset in datasets:
    dataset_name = dataset.split('/')[-1]  # Extract the dataset name from the full path

    # Check if the zip file already exists, if not, download it
    if f"{dataset_name}.zip" not in os.listdir(destination_folder):
        # Download the dataset from Kaggle into memory
        api.dataset_download_files(dataset, path=destination_folder, force=True)
        print(f"Downloaded {dataset} from Kaggle as {dataset_name}")
    else:
        print(f"Dataset {dataset_name} already exists, skipping download of the zip file")

# Load the uploaded files from the checkpoint file
uploaded_files = json.load(open(checkpoint_file, 'r'))

# Extract the files from the zip archive
with ZipFile(os.path.join(destination_folder, f"{dataset_name}.zip")) as zip_file:
    # Upload each file to S3
    for file in zip_file.namelist():
        # Skip directories
        if not file.endswith('/'):
            s3_file_key = f"{s3_directory}/{dataset_name}/{file}"
            if s3_file_key in uploaded_files:
                print(f"Skipping already uploaded file: {file}")
            else:
                # Attempt to upload the file to S3
                try:
                    # Read file content
                    file_content = zip_file.read(file)
                    # Upload the file to S3
                    s3.put_object(Body=file_content, Bucket=s3_bucket_name, Key=s3_file_key)
                    print(f"Uploaded {file} to S3://{s3_bucket_name}/{s3_file_key}")
                    # Update the list of uploaded files
                    uploaded_files.append(s3_file_key)
                except Exception as e:
                    print(f"""There was an error: {e}
Starting saving the checkpoint file...
                          """)

# Update the checkpoint file after processing all files
with open(checkpoint_file, 'w') as f:
    json.dump(uploaded_files, f)

print("Checkpoint file updated")

  