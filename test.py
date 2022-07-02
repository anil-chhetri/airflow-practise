# set connection string
# get the connection string from sas azure portal

# export AZURE_STORAGE_CONNECTION_STRING="<yourconnectionstring>"
import os

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Retrieve the connection string for use with the application. The storage
# connection string is stored in an environment variable on the machine
# running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
# created after the application is launched in a console or with Visual Studio,
# the shell or application needs to be closed and reloaded to take the
# environment variable into account.
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
airflow_home = os.environ.get("AIRFLOW_HOME")

print('connection started.')

blob_service_client = BlobServiceClient.from_connection_string(connect_str)

upload_file_path = f"{airflow_home}/data/yellow_tripdata_2019-01.parquet"

# Create a blob client using the local file name as the name for the blob
blob_client = blob_service_client.get_blob_client(
    container='datastores', blob="yellow_tripdata_2019-01.parquet")

print('blob created.')


with open(upload_file_path, "rb") as data:
    blob_client.upload_blob(data)

print('complteed.')
