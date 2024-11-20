import logging
import azure.functions as func
import os
import gzip 
import requests
from azure.storage.blob import BlobServiceClient

# Set up environment variables
SAS_TOKEN = os.getenv('SAS_TOKEN')
HTTP_ENDPOINT = os.getenv('HTTP_ENDPOINT')
API_KEY = os.getenv('API_KEY')

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
def henkelforwarder(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')

    try:
        # Parse the Event Grid event
        event_data = azeventgrid.get_json()
        logging.info(f"Event Data: {event_data}")

        topic = azeventgrid.topic  # The topic contains the storage account info
        storage_account_name = topic.split("/")[8]
        logging.info(f"Storage Account Name: {storage_account_name}")

        # Access the blob URL from the `url` field under `data`
        blob_url = event_data.get("url")  # Adjusted based on the actual event structure
        logging.info(f"Blob URL: {blob_url}")
        if not blob_url:
            logging.error("No blob URL found in the event data.")
            return
        logging.info(f"Processing blob URL: {blob_url}")

        # Download the blob
        blob_content = download_blob(blob_url, storage_account_name)

        # Process the gz file
        logs = decompress_log(blob_content)

        for log in logs:
            if log.strip():
                send_log_to_http(log)

    except Exception as e:
        logging.error(f"Error processing event: {e}")


def download_blob(blob_url, storage_account_name):
    blob_service_url = f"https://{storage_account_name}.blob.core.windows.net"
    blob_service_client = BlobServiceClient(account_url=blob_service_url, credential=SAS_TOKEN)
    
    # Parse container and blob name from URL
    container_name, blob_name = parse_blob_url(blob_url, storage_account_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    download_stream = blob_client.download_blob()
    return download_stream.readall()

def parse_blob_url(blob_url, storage_account_name):
    parsed_url = blob_url.replace(f"https://{storage_account_name}.blob.core.windows.net/", "")
    parts = parsed_url.split("/")
    container_name = parts[0]
    blob_name = "/".join(parts[1:])
    return container_name, blob_name

def decompress_log(blob_content):
    decompressed_content = gzip.decompress(blob_content).decode('utf-8')
    logs = decompressed_content.split('\n')
    return logs

def send_log_to_http(log):
    try:
        headers = {'Content-Type': 'application/json'}
        headers['X-API-Key'] = API_KEY

        response = requests.post(HTTP_ENDPOINT, data=log, headers=headers)
        response.raise_for_status()
        logging.info(f"Log forwarded successfully to HTTP endpoint: {log}")
    except Exception as e:
        logging.error(f"Failed to forward log to HTTP endpoint, error: {e}")