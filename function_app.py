import logging
import azure.functions as func
import os
import gzip
import requests
from azure.storage.blob import BlobServiceClient
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List

# Set up environment variables
SAS_TOKEN = os.getenv('SAS_TOKEN')
HTTP_ENDPOINT = os.getenv('HTTP_ENDPOINT')
API_KEY = os.getenv('API_KEY')

app = func.FunctionApp()

@app.event_grid_trigger(arg_name="azeventgrid")
def henkelforwarder(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')

    # Validate critical environment variables
    if not SAS_TOKEN or not HTTP_ENDPOINT or not API_KEY:
        raise ValueError("Missing required environment variables: SAS_TOKEN, HTTP_ENDPOINT, or API_KEY")

    try:
        # Parse the Event Grid event
        event_data = azeventgrid.get_json()
        logging.info(f"Event Data: {event_data}")

        topic = azeventgrid.topic  # The topic contains the storage account info
        storage_account_name = topic.split("/")[8]
        logging.info(f"Storage Account Name: {storage_account_name}")

        # Access the blob URL from the `url` field under `data`
        blob_url = event_data.get("data", {}).get("url")  # Ensure to access the correct nested structure
        if not blob_url:
            logging.error("No blob URL found in the event data.")
            return
        logging.info(f"Processing blob URL: {blob_url}")

        # Download the blob
        blob_content = download_blob(blob_url, storage_account_name)

        # Process the gzipped logs
        logs = decompress_log(blob_content)

        # Send each log line to the HTTP endpoint
        for log in logs:
            if log.strip():  # Ignore empty lines
                send_log_to_http(log)

    except Exception as e:
        logging.error(f"Error processing event: {e}")

def download_blob(blob_url: str, storage_account_name: str) -> bytes:
    """Download the blob content."""
    try:
        blob_service_url = f"https://{storage_account_name}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=blob_service_url, credential=SAS_TOKEN)
        
        # Parse container and blob name from URL
        container_name, blob_name = parse_blob_url(blob_url, storage_account_name)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        download_stream = blob_client.download_blob()
        logging.info(f"Downloaded blob: {blob_name}, Size: {download_stream.properties.size} bytes")
        return download_stream.readall()
    except Exception as e:
        logging.error(f"Failed to download blob from URL {blob_url}: {e}")
        raise

def parse_blob_url(blob_url: str, storage_account_name: str) -> (str, str):
    """Parse the blob URL to extract container and blob name."""
    parsed_url = blob_url.replace(f"https://{storage_account_name}.blob.core.windows.net/", "")
    parts = parsed_url.split("/")
    container_name = parts[0]
    blob_name = "/".join(parts[1:])
    return container_name, blob_name

def decompress_log(blob_content: bytes) -> List[str]:
    """Decompress gzipped log content into a list of log lines."""
    try:
        decompressed_content = gzip.decompress(blob_content).decode('utf-8')
        return decompressed_content.split('\n')
    except Exception as e:
        logging.error(f"Error decompressing log content: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def send_log_to_http(log: str):
    """Send a single log entry to the HTTP endpoint with retry logic."""
    try:
        headers = {
            'Content-Type': 'application/json',
            'X-API-Key': API_KEY
        }
        response = requests.post(HTTP_ENDPOINT, data=log, headers=headers)
        response.raise_for_status()
        logging.info(f"Log successfully sent to endpoint: {HTTP_ENDPOINT}")
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request failed: {e}")