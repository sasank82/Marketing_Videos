import os
import logging
from google.cloud import storage
from config_loader import service_account_key_path
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


logger = logging.getLogger(__name__)

from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))  # Retries up to 3 times with exponential backoff
def download_from_gcs(bucket_name, blob_name, local_path):
    """
    Downloads a file (blob) from Google Cloud Storage (GCS).
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        blob_name (str): The name of the blob to download.
        local_path (str): The local path where the file should be saved.
        service_account_key_path (str): Path to the Google Cloud service account key.

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    try:
        storage_client = storage.Client.from_service_account_json(service_account_key_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(local_path)
        logger.info(f"Downloaded {blob_name} to {local_path}.")
        return True
    except Exception as e:
        logger.error(f"Error downloading {blob_name} from GCS: {e}")
        raise  # Re-raise exception to trigger retry

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))  # Retries up to 3 times with exponential backoff
def upload_to_gcs(bucket_name, file_path):
    """
    Uploads a file to Google Cloud Storage (GCS).
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        file_path (str): The local path to the file to upload.
        service_account_key_path (str): Path to the Google Cloud service account key.

    Returns:
        str: URL of the uploaded file on GCS, or None if an error occurs.
    """
    try:
        storage_client = storage.Client.from_service_account_json(service_account_key_path)
        bucket = storage_client.bucket(bucket_name)
        blob_name = os.path.basename(file_path)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_filename(file_path)
        #logger.info(f"Uploaded {file_path} to {bucket_name}/{blob_name}.")
        
        return f"https://storage.cloud.google.com/{bucket_name}/{blob_name}"
    
    except Exception as e:
        logger.error(f"Error uploading {file_path} to GCS: {e}")
        raise  # Re-raise exception to trigger retry

def manage_blob(bucket_name, blob_name, delete=False):
    """
    Checks if a blob exists in the specified bucket and optionally deletes it.
    
    Args:
        bucket_name (str): The name of the GCS bucket.
        blob_name (str): The name of the blob to check or delete.
        service_account_key_path (str): Path to the Google Cloud service account key.
        delete (bool): If True, deletes the blob if it exists.

    Returns:
        bool: True if the blob exists (or was deleted), False otherwise.
    """
    try:
        storage_client = storage.Client.from_service_account_json(service_account_key_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if blob.exists():
            if delete:
                blob.delete()
                logger.info(f"Blob {blob_name} deleted successfully.")
            return True
        else:
            logger.info(f"Blob {blob_name} does not exist.")
            return False
    
    except Exception as e:
        logger.error(f"Error managing blob {blob_name}: {e}")
        return False

import time
from googleapiclient.errors import HttpError

class GoogleSheetsManager:
    def __init__(self, service_account_key_path, sheet_id):
        """
        Initializes the GoogleSheetsManager with service account credentials and sheet ID.
        
        :param service_account_key_path: Path to the service account key JSON file.
        :param sheet_id: The ID of the Google Sheet.
        """
        self.sheet_id = sheet_id
        self.service = self._get_sheets_service(service_account_key_path)

    def _get_sheets_service(self, service_account_key_path):
        """
        Returns an authenticated Google Sheets API service instance.
        
        :param service_account_key_path: Path to the service account key JSON file.
        :return: Google Sheets API service instance.
        """
        credentials = Credentials.from_service_account_file(
            service_account_key_path, 
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build('sheets', 'v4', credentials=credentials)
        return service

    def create_sheet_if_not_exists(self, sheet_name):
        """
        Creates a new sheet (tab) with the provided name if it does not exist in the spreadsheet.
        
        :param sheet_name: The name of the sheet (tab) to create.
        """
        # Check if the sheet already exists
        sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.sheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        
        for sheet in sheets:
            if sheet.get("properties", {}).get("title") == sheet_name:
                return  # Sheet already exists, no need to create
        
        # If the sheet doesn't exist, create it
        requests = [{
            'addSheet': {
                'properties': {
                    'title': sheet_name
                }
            }
        }]
        
        body = {
            'requests': requests
        }
        
        self.service.spreadsheets().batchUpdate(
            spreadsheetId=self.sheet_id,
            body=body
        ).execute()

    def clear_sheet(self, sheet_name):
        """
        Clears all the data from the sheet (tab) with the provided sheet_name.
        
        :param sheet_name: The name of the sheet (tab) to clear.
        """
        # Define the range to clear
        range_ = f"'{sheet_name}'!A1:Z1000"  # Adjust the range according to your needs
        clear_values_request_body = {}
        
        # Clear the sheet contents
        self.service.spreadsheets().values().clear(
            spreadsheetId=self.sheet_id, 
            range=range_, 
            body=clear_values_request_body
        ).execute()

    def log_to_sheet(self, data, sheet_name):
        """
        Logs data to Google Sheet in a specific sheet (tab) without retries.
        
        :param data: The data to log (as a list of values).
        :param sheet_name: The name of the sheet (tab) to log data into.
        """
        # Ensure the sheet exists
        self.create_sheet_if_not_exists(sheet_name)

        # Define the range for appending data
        range_ = f"'{sheet_name}'!A1:Z"  # Append to the next available row in the sheet
        value_input_option = 'RAW'

        # Prepare the data to append
        values = [data]
        body = {'values': values}

        try:
            # Append the data to the specific sheet
            self.service.spreadsheets().values().append(
                spreadsheetId=self.sheet_id,
                range=range_,
                valueInputOption=value_input_option,
                insertDataOption="INSERT_ROWS",  # This ensures new data is appended
                body=body
            ).execute()
            logger.info(f"Successfully logged {data} to Google Sheets in sheet {sheet_name}")
        except HttpError as e:
            logger.error(f"Failed to log data to Google Sheets: {e}")
            raise


    def log_failure(self, user_phone, error_message, sheet_name="Failures"):
        """
        Logs failure details to Google Sheet in a specific failure sheet (tab).
        
        :param user_phone: The phone number of the user.
        :param error_message: The error message to log.
        :param sheet_name: The name of the failure sheet (tab), defaults to "Failures".
        :param retries: Number of times to retry in case of failure.
        """
        # Prepare the data (e.g., user_phone and error_message)
        data = [user_phone, "FAILURE", error_message]

        # Log failure to the specified sheet (or a default one) with retries
        self.log_to_sheet(data, sheet_name)
