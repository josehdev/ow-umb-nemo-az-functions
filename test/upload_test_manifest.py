#!/usr/bin/env python3

"""
This test utility simulates what the NeMO website would do to upload a manifest file:
    - Take a test manifest file (arg.file) and upload it to the Nemo ingest manifest container 
      with the expected file name structure
    - Creates the blob metadata for the uploaded manifest
    - Insert a new submissions record in the submissions db
"""

import argparse
import random
import configparser
import json
from datetime import datetime
import os
import requests
import sys
import mysql.connector
from azure.storage.blob import BlobServiceClient


# Read config file
confpath = os.path.join(os.path.dirname(__file__), 'test_conf.ini')
config = configparser.ConfigParser()
config.read(confpath)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload a manifest file to NeMO ingest container."
    )

    parser.add_argument(
        "--file", "-f",
        help="The path of the manifest file to upload.",
        type=str,
        default=config['metadata']['testfile']
    )

    args = parser.parse_args()

    return args


def generate_submission_id():
    # All lower case characters, but Exclude those that are commonly
    # mistaken for others such as 1, l, o, and 0
    valid = "23456789abcdefghijkmnopqrstuvwxyz"
    number_of_valid = len(valid)

    sub_id = ""

    for _ in range(7):
        pick = random.randint(0, number_of_valid - 1)
        chosen = valid[pick]
        sub_id += chosen

    return sub_id


def update_submission_db(data: dict):
    try:
        conn = mysql.connector.connect(
            user=config['db']['username'],
            passwd=config['db']['password'],
            host=config['db']['host'],
            database=config['db']['database']
        )

        cmd = """
        INSERT INTO submissions
        (submission_id, submitter, creation_dt)
        VALUES (%(submission_id)s, %(submitter)s, now())
        """

        params = {
            'submission_id': data['submission_id'],
            'submitter': data['nemo_submitter']
        }

        cursor = conn.cursor()
        cursor.execute(cmd, params)
        conn.commit()

    except Exception as err:
        conn.rollback()
        print("Unable to update_submission_db: ", exc_info=True)
    finally:
        cursor.close()
        conn.close()


def upload_manifest(filepath: str):
    # Create the new file name
    timestamp = datetime.now().strftime('%Y-%m-%d-%H_%M_%S')
    sub_id = generate_submission_id()
    extension = 'tsv'
    new_name = f'manifest-{timestamp}-{sub_id}.{extension}'

    # Create metadata
    metadata = {
        'nemo_submitter':config['metadata']['submitter'],
        'nemo_submitter_first':config['metadata']['submitter_first'],
        'nemo_submitter_last':config['metadata']['submitter_last'],
        'nemo_submitter_email':config['metadata']['submitter_email'],
        'original_name':os.path.basename(filepath),
        'submission_id':sub_id,
        'submitted_via':'upload_test'
    }    

    # Upload file 
    blobServiceClient = BlobServiceClient.from_connection_string(
        config['storage-nemo']['azure-storage-connection-string']
    )
    container_name = config['storage-nemo']['manifest-container']
    blob = blobServiceClient.get_blob_client(container_name, new_name)

    filepath = os.path.join(os.path.dirname(__file__), filepath)
    with open(filepath, "rb") as file:
        blob.upload_blob(file, metadata=metadata, overwrite=True)

    # Insert row in Submissions table
    update_submission_db(metadata)


def main():
    args = parse_args()

    filepath = args.file

    upload_manifest(filepath)

main()