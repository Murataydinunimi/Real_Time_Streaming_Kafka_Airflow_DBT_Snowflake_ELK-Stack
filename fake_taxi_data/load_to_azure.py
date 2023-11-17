import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
from dotenv import load_dotenv
import json
load_dotenv()
from datetime import datetime

storage_account_name = os.getenv("storage_account_name")
storage_account_key = os.getenv("storage_account_key")
connection_string = os.getenv("connection_string")

def load_to_azure(flattened_messages,container_name,blob_name):

    #current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    #blob_name = f"{current_timestamp}_transactions.json"

    list_of_messages = json.dumps(flattened_messages)


    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)

    blob_client.upload_blob(list_of_messages)

    print(f"Uploaded file to {blob_client.url}")
