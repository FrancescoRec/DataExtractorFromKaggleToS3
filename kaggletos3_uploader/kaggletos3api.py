import os
import boto3
from zipfile import ZipFile

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

# Specify the datasets you want to download from Kaggle
datasets = [
    'rtatman/digidb',
    'juniorbueno/opencv-facial-recognition-lbph',
    'otherdataset/blabla'
    '...and so on...'
]

# Specify the destination folder for downloading and extracting the ZIP file
destination_folder = "./datasets" # Change this to the folder where you want to download the datasets
if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)

for dataset in datasets:
    # Download the dataset from Kaggle into memory
    api.dataset_download_files(dataset, path=destination_folder, force=True)
    dataset_name = dataset.split('/')[-1] # Extract the dataset name from the full path
    print(f"Downloaded {dataset} from Kaggle as {dataset_name}")

    # Extract the files from the zip archive
    with ZipFile(os.path.join(destination_folder, f"{dataset_name}.zip")) as zip_file:
        # Upload each file to S3
        for file in zip_file.namelist():
            # Skip directories
            if not file.endswith('/'):
                file_content = zip_file.read(file)
                # Construct S3 key by removing the first folder (assuming there's only one top-level folder)
                s3_file_key = f"{s3_directory}/{dataset_name}/{file}"
                s3.put_object(Body=file_content, Bucket=s3_bucket_name, Key=s3_file_key)
                print(f"Uploaded {file} to S3://{s3_bucket_name}/{s3_file_key}")
            else:
                print(f"Skipping directory: {file}")

