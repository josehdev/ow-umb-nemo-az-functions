# Azure Function Blueprint

import azure.functions as func
import logging
import tempfile
import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

old_function_validate_manifest = func.Blueprint()

@old_function_validate_manifest.blob_trigger(arg_name="blobevent", path="nemo-manifest-files",
                                            connection="CONTENT_STORAGE_ACCOUNT") 
def validate_manifest(blobevent: func.InputStream):
    logging.info(f"Python blob trigger function validate_manifest processed blob"
                f"Name: {blobevent.name}"
                f"Blob Size: {blobevent.length} bytes")
    
    file_name = os.path.basename(blobevent.name)

    manifest_container = os.getenv("AZURE_MANIFEST_CONTAINER") 
    azure_storage_account = os.environ['AZURE_STORAGE_ACCOUNT']
    credential = DefaultAzureCredential()

    client = BlobServiceClient.from_connection_string(azure_storage_account, DefaultAzureCredential())

    container_client = client.get_container_client(manifest_container)
    blob = container_client.get_blob_client(file_name)

    custom_metadata = blob.get_blob_properties().metadata

    if not custom_metadata:
        my_metadata = {'MyMetadata_1':'MyValue_1'}
        blob.set_blob_metadata(my_metadata)


    # Download the blob to a temporary file
    tf = tempfile.NamedTemporaryFile(delete = False)
   # tf.close()  #In Windows it's necessary to close the file to be able to reopen it several times.

    with open(tf.name, mode="wb") as file:
        stream = blob.download_blob()
        file.write(stream.readall())

    try:
        with open(tf.name, 'r', encoding='utf-8') as file:
            # Try reading the file as UTF-8
            file.read()
    except UnicodeDecodeError:
        try:
            with open(tf.name, 'r', encoding='ascii') as file:
                # Try reading the file as ASCII
                file.read()
        except UnicodeDecodeError:
            raise Exception("File not encoded in UTF-8 or ASCII")

    logging.info('File was read')


