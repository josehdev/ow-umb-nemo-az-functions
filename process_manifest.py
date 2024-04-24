import json
import os
import sys
import pika
import re
import tempfile
import traceback
import azure.functions as func
import logging

from manifest import Manifest
from validation_checks import validate_manifest, BICAN_COLUMNS
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient

""""
Original module created by: UMB
This version created by: 
Jose Herrera (jherrera@oakwoodsys.com)
Oakwood Systems Group, Inc. (www.oakwoodsys.com/)
Project: Nemo Ingest into Azure Blob Storage 
April 2024

"""

# This Azure Function is defined as a Blueprint.
# To find the main function definition, search for this variable in this file
function_process_manifest = func.Blueprint()


# How many errors to report. The rest will be available in an
# error file in the bucket, that we will report the path to.
error_count_to_report = 30

azure_storage_account_connection = os.environ['AZURE_STORAGE_ACCOUNT_CONNECTION']
blob_service_uri = os.environ['CONTENT_STORAGE_ACCOUNT__blobServiceUri']

project_name = os.getenv("PROJECT_NAME")

manifest_container = os.getenv("AZURE_MANIFEST_CONTAINER")
manifest_error_container = os.getenv("AZURE_MANIFEST_ERROR_CONTAINER")
nemo_aux_files_container = 'nemo-aux-files'

# Files read in from nemo-aux-files bucket
controlled_vocab_filename = os.getenv("AUX_FILENAME_CONTROLLED_VOCAB")
ic_form_mapping_filename = os.getenv("AUX_FILENAME_IC_FORM_MAPPING")
restricted_bucket_list_filename = os.getenv("AUX_FILENAME_RESTRICTED_BUCKET_LIST")

# BICAN CV terms. Only pulled in if the manifest is a BICAN manifest
bican_controlled_vocab_filename = os.getenv("AUX_FILENAME_BICAN_CONTROLLED_VOCAB")


def get_blob_service_client():
    """
    Creates and returns a Blob Service client authenticated with Azure.
    """    
    credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient.from_connection_string(azure_storage_account_connection, credential)

    return blob_service_client


def get_blob_client(client: BlobServiceClient, ct_name: str, blob_name: str):
    """
    Returns a Blob Client for the specified blob name.
    """    
    container_client = client.get_container_client(ct_name)
    blob_client = container_client.get_blob_client(blob_name)

    return blob_client


def download_to_filename(blob: BlobClient, filename: str):
    """
    Downloads a Blob into a local file.
    """    
    with open(filename, mode="wb") as file:
        stream = blob.download_blob()
        # Download by chunks to prevent max out memory usage 
        for chunk in stream.chunks():
            file.write(chunk)


def verify_valid_encoding(filename):
    logging.info("In verify_valid_encoding().")
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            # Try reading the file as UTF-8
            file.read()
    except UnicodeDecodeError:
        try:
            with open(filename, 'r', encoding='ascii') as file:
                # Try reading the file as ASCII
                file.read()
        except UnicodeDecodeError:
            raise Exception("File not encoded in UTF-8 or ASCII")


def confirm_file_metadata_is_present(metadata):
    logging.info("In confirm_file_metadata_is_present().")

    all_metadata_present = True
    custom_metadata_fields = [
        'nemo_submitter',
        'nemo_submitter_first',
        'nemo_submitter_last',
        'nemo_submitter_email',
        'original_name',
        'submission_id'
    ]

    if metadata is None:
        # Custom metadata object is missing
        all_metadata_present = False
    else:
        # Metadata object is present.

        # Confirm the required fields are present
        for field in custom_metadata_fields:
            if field not in metadata:
                all_metadata_present = False
                break

            # The field are present. Now confirm the field has a value.
            value = metadata[field]

            if value is None or len(value) == 0:
                all_metadata_present = False
                break

    logging.info(f"All metadata present: {all_metadata_present}")

    return all_metadata_present


def get_from_aux_files_container(client: BlobServiceClient, blob_name: str):
    logging.info("In get_from_aux_files_container().")

    blob = get_blob_client(client, nemo_aux_files_container, blob_name)
    data = json.loads(blob.download_blob().readall())

    return data


def get_manifest_headers(file_name):
    """
    Returns the manifest column headers as a list.
    """
    try:
        with open(file_name, encoding='utf-8-sig', mode='r') as f:
            headers = f.readline().strip('\n')
            headers = headers.split('\t')
            return headers
    except Exception:
        return None


def get_rabbitmq_channel(rabbitmq_connection, exchange_name, queue_name, routing_key):
    """
    Returns a RabbitMQ pika.channel.Channel
    """
    logging.info("In get_rabbitmq_channel().")

    channel = rabbitmq_connection.channel()

    channel.exchange_declare(exchange=exchange_name,
                             exchange_type="direct",
                             durable=True)

    channel.queue_declare(queue=queue_name,
                          durable=True,
                          arguments={"x-single-active-consumer": True})

    # Establish relationship between exchange and queue
    channel.queue_bind(exchange=exchange_name,
                       queue=queue_name,
                       routing_key=routing_key)

    return channel


def handle_error_condition(client, manifest_name, errors, metadata):
    logging.info("In handle_error_condition().")
    error_file = manifest_name + '.errors'
    error_blob = get_blob_client(client, manifest_error_container, error_file)

    # Set the metadata for the error file
    error_count = len(errors)
    logging.info(f"Number of errors: {error_count}.")

    if error_count > 0:
        error_string = "\n".join(errors)
    else:
        error_string = "No errors"

    error_blob.upload_blob(error_string, metadata=metadata, overwrite=True)
    azure_error_path = f'{blob_service_uri}/{manifest_error_container}/{error_file}'

    logging.info(f"Errors available at {azure_error_path}.")

    xtra_payload = {}

    if len(errors) > error_count_to_report:
        # Slice out the top N errors
        errors_to_report = errors[:error_count_to_report]
        xtra_payload['errors'] = errors_to_report
        xtra_payload['complete_errors'] = False
    else:
        xtra_payload['errors'] = errors
        xtra_payload['complete_errors'] = True

    xtra_payload['gcp_error_path'] = azure_error_path

    logging.info("Returning payload from handle_error_condition.")

    return xtra_payload


def notify_nemo(payload):
    """
    Pushes the JSON payload to the RabbitMQ queue.
    This queues the NeMO submission for the next step in ingest.
    """
    logging.info("In notify_nemo().")

    payload_str = json.dumps(payload)
    logging.info("payload:\n{}".format(payload_str, indent=2))

    try:
        # Get RabbitMQ configuration from environment
        rabbitmq_host = os.getenv("RABBITMQ_HOST")
        rabbitmq_port =  int(os.getenv("RABBITMQ_PORT"))
        rabbitmq_virtual_host =  os.getenv("RABBITMQ_VIRTUAL_HOST")
        rabbitmq_username = os.getenv("RABBITMQ_USERNAME")
        rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
        publisher_exchange_name = os.getenv("RABBITMQ_PUBLISHER_EXCHANGE_NAME")
        publisher_queue_name = os.getenv("RABBITMQ_PUBLISHER_QUEUE_NAME")
        publisher_routing_key = os.getenv("RABBITMQ_PUBLISHER_ROUTING_KEY")

        # RabbitMQ credentials for publisher
        rabbitmq_credentials = pika.PlainCredentials(
            username=rabbitmq_username,
            password=rabbitmq_password
        )

        # Parameters for RabbitMQ producer
        cxn_parameters = pika.ConnectionParameters(
            host=rabbitmq_host,
            port=rabbitmq_port,
            virtual_host=rabbitmq_virtual_host,
            credentials=rabbitmq_credentials
        )

        # Get connection to RabbitMQ instance on GCP
        rabbitmq_connection = pika.BlockingConnection(
            parameters=cxn_parameters
        )

        # Get a connection channel.
        channel = get_rabbitmq_channel(
            rabbitmq_connection,
            publisher_exchange_name,
            publisher_queue_name,
            publisher_routing_key
        )

        # Publish message to the next step's queue.
        channel.basic_publish(
            exchange=publisher_exchange_name,
            routing_key=publisher_routing_key,
            body=payload_str,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

        rabbitmq_connection.close()
        logging.info(f"RabbitMQ message published to {publisher_queue_name} queue.")
    except Exception:
        tb_message = traceback.format_exc()
        logging.info(f"An error occurred while publishing message to RabbitMQ: {tb_message}")


# If the manifest file has such problems that we can't really even do a proper
# validation, then skip the validation, notify RabbitMQ, and abort.
def handle_bad_manifest_file(submission_metadata, error):
    logging.info("In handle_bad_manifest_file().")

    submission_id = submission_metadata['submission_id']
    submitter = submission_metadata['submitter']
    first = submission_metadata['first']
    last = submission_metadata['last']
    email = submission_metadata['email']
    dryrun = submission_metadata['dryrun']
    cloud_path = submission_metadata['cloud_path']
    original_filename = submission_metadata['original_filename']

    if error is None:
        errors = []
    else:
        errors = [error]

    # NOTE: Intentionally dropped 'complete_errors' and 'error_path'
    # from paylaod to target appropriate handling in manifest validation
    # listener.
    payload = {
        'submission_id': submission_id,
        'manifest_path': cloud_path,
        'project_name': project_name,
        'original_filename': original_filename,
        'submitter': {
            'username': submitter,
            'first': first,
            'last': last,
            'email': email
        },
        'errors': errors,
        'program': "unknown",
        'result': False,
        'dryrun': dryrun
    }

    notify_nemo(payload)


def make_error_metadata(submission_metadata):
    logging.info("In make_error_metadata().")
    # Keys to exclude from the copy
    keys_to_exclude = {'original_filename', 'cloud_path'}

    # Use a dictionary comprehension to make a copy of submission_data,
    # but exclude the keys in keys_to_exclude
    new_dict = {
        key: value
        for key, value in submission_metadata.items()
        if key not in keys_to_exclude
    }

    return new_dict


@function_process_manifest.blob_trigger(arg_name="blobevent", path=manifest_container,
                                        connection="CONTENT_STORAGE_ACCOUNT") 
def process_manifest(blobevent: func.InputStream):
    """
    Triggered by a change to a Blob Storage Container.
    Args:
         blobevent (InputStream): Event blob stream.
    """
    valid = False
    dryrun = False
    access_level = None
    program = "unknown"
    errors = []
    file_name = os.path.basename(blobevent.name)

    client = get_blob_service_client()

    logging.info(f"Processing file: {file_name}.")

    blob = get_blob_client(client, manifest_container, file_name)
    custom_metadata = blob.get_blob_properties().metadata

    # Confirm all file metadata is present before continuing
    is_metadata_present = confirm_file_metadata_is_present(custom_metadata)

    if not is_metadata_present:
        # Gracefully exit if any metadata is absent
        logging.info("ERROR: Custom metadata is missing from the file blob. " + \
                    f"Done processing file: {file_name}.")
        return

    # Dryrun is an optional flag. If present check for a truthy value
    if 'dryrun' in custom_metadata:
        if custom_metadata['dryrun'] in ['yes', 'true', True]:
            dryrun = True

    # The name is of the form: manifest-{date}-{time}-{submission_id}.{ext}
    # Example: manifest-2021-09-01-11:31:54-NaYAbB2.csv
    azure_path = blob.url

    # Download the blob to a temporary file
    tf = tempfile.NamedTemporaryFile(delete = 'linux' in sys.platform.lower())
    download_to_filename(blob, tf.name)

    logging.info(f"Successfully downloaded {file_name} from {manifest_container} to {tf.name}.")

    # Get the submitter and other metadata. We shouldn't rely on the filename
    # for this data...
    submitter = custom_metadata['nemo_submitter']
    submitter_first = custom_metadata['nemo_submitter_first']
    submitter_last = custom_metadata['nemo_submitter_last']
    submitter_email = custom_metadata['nemo_submitter_email']
    submission_id = custom_metadata['submission_id']

    # This is the name of the manifest as it was originally on the submitter's
    # browser.
    original_filename = custom_metadata['original_name']

    submission_metadata = {
        "submission_id": submission_id,
        "submitter": submitter,
        "dryrun": str(dryrun),
        "first": submitter_first,
        "last": submitter_last,
        "email": submitter_email,
        "original_filename": original_filename,
        "cloud_path": azure_path
    }

    logging.info(f"Manifest submitted via the NeMO website by: {submitter}.")

    try:
        verify_valid_encoding(tf.name)
    except Exception:
        logging.info("ERROR: File of incorrect type or encoding.")
        error = "File must be a UTF-8 or ASCII encoded plaintext tsv file."
        handle_bad_manifest_file(submission_metadata, error)
        return

    # Expected manifest name: manifest-YYYY-MM-DD-HH:MM:SS-abde123.tsv
    filename_pattern = r'manifest\-[0-9]{4}\-[0-9]{2}\-[0-9]{2}\-[0-9]{2}[:_][0-9]{2}[:_][0-9]{2}\-[a-zA-z0-9]{7}\.tsv'

    # If filename is not what we expect, abort the validation, but make a notification
    if not re.match(filename_pattern, file_name):
        logging.info(f"ERROR: Manifest name {file_name} does not match expected " + \
              "naming convention. It is possible the initial upload to " + \
              "container was interrupted.")
        handle_bad_manifest_file(submission_metadata, None)
        return

    # Get the column headers of the submitted manifest
    manifest_headers = get_manifest_headers(tf.name)

    # If no column headers are returned, assume the file in encoded incorrectly
    # or has in not in TSV format.
    if manifest_headers is None:
        errors.append("No valid manifest TSV header row found.")

    # Does the submitted manifest have all the BICAN columns?
    elif all(field in manifest_headers for field in BICAN_COLUMNS):
        # BICAN manifests only #
        logging.info("BICAN manifest detected.")
        program = 'bican'

        cv = get_from_aux_files_container(client, bican_controlled_vocab_filename)

        # Do BICAN validation checks
        errors = validate_manifest(tf.name, cv)

        if len(errors) == 0:
            valid = True
    else:
        ## Non-BICAN manifests ##

        # Load non-BICAN controlled vocab terms
        cv = get_from_aux_files_container(client, controlled_vocab_filename)

        # Analyze the submitted manifest for errors.
        manifest = Manifest(tf.name, cv)

        # Read manifest. Doing this here let's us pass the data to the restricted
        # validation if the submission passes the 1st set of validation and is
        # destined to upload files to restricted containers.
        manifest_data = manifest.read_file()

        if manifest_data:
            # Manifest could be read so we can proceed with validation
            valid = manifest.validate_manifest_file(manifest_data)

            access_level = manifest.access_level
            program = manifest.program

            # Confirm program is a valid CV, otherwise it set to 'unknown'.
            if program not in cv["Program"]:
                program = "unknown"

        if valid:
            # If biccn and restricted, confirm restricted buckets exist for the
            # submission.
            # NOTE: Will eventually remove biccn filter and will check for all restricted buckets.
            if access_level == "controlled" and program == "biccn":
                # Get IC form to collection, project, an CA usage mapping
                ic_mapping = get_from_aux_files_container(client, ic_form_mapping_filename)

                # Get restricted bucket listing
                listing_data = get_from_aux_files_container(client, restricted_bucket_list_filename)
                bucket_list = listing_data["bucket_list"]

                valid = manifest.validate_restricted_manifest_fields(
                        manifest_data, ic_mapping, bucket_list)

        errors = manifest.errors

    if valid:
        logging.info("VALID")
    else:
        logging.info("INVALID")

    # Now put together the payload to call NeMO back with the results.
    payload = {
        'submission_id': submission_id,
        'manifest_path': azure_path,
        'project_name': project_name,
        'original_filename': original_filename,
        'submitter': {
            'username': submitter,
            'first': submitter_first,
            'last': submitter_last,
            'email': submitter_email
        },
        'program': program,
        'result': valid,
        'dryrun': dryrun
    }

    # Add any errors to the payload.
    # There may be a huge number, so don't blindly add them all.
    # Truncate and add a note that not all are there.
    if len(errors) > 0:
        logging.info(f"Number of errors: {len(errors)}.")
        try:
            err_meta = make_error_metadata(submission_metadata)
            xtra_payload = handle_error_condition(client, file_name, errors, err_meta)
        except Exception as err:
            # If the error file cannot be uploaded, the submissions will be
            # processed the same way mismatched manifest names are by the
            # manifest validation listener. Also see NOTE in the above block.
            logging.info(f"An error occurred in handle_error_condition(): {err}")
            xtra_payload = {
                'errors': []
            }

        # Merge the extra information into the existing payload
        payload.update(xtra_payload)

    notify_nemo(payload)
