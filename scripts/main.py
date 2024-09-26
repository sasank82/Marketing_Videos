import os
import logging
from functools import partial
from multiprocessing import Manager, Pool, cpu_count
from dotenv import load_dotenv
from user_info_manager import get_customer_info
from user_worker import generate_the_needful_for_users
from config_loader import customer_info_sheet, audio_config_path, client_name, customer_info_mapping_path, video_bucket_name, service_account_key_path, google_sheet_id
from utils import monitor_memory_usage, read_configuration
from gcs_utils import upload_to_gcs, GoogleSheetsManager
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function to orchestrate voiceover generation, video generation, and file uploads.
    """
    try:
        # Monitor initial memory usage
        monitor_memory_usage("Initial memory usage")

        # Load customer info mapping from the config
        if not os.path.exists(customer_info_mapping_path):
            raise FileNotFoundError(f"Customer info mapping file not found: {customer_info_mapping_path}")
        
        with open(customer_info_mapping_path, 'r', encoding="utf-8") as f:
            customer_info_mapping = json.load(f)
        
        logger.info(f"Loaded customer info mapping: {customer_info_mapping_path}")

        start_row = int(os.getenv('START_ROW', 154))
        end_row = int(os.getenv('END_ROW', 154))

        # Generate a unique sheet (tab) name for this container based on its row range
        container_specific_tab = f"{client_name}_rows_{start_row}_{end_row}"

        # Generate user details based on customer info sheet and mapping
        if not os.path.exists(customer_info_sheet):
            raise FileNotFoundError(f"Customer info sheet file not found: {customer_info_sheet}")
        
        user_details = get_customer_info(customer_info_sheet, customer_info_mapping, start_row, end_row)

        # Load audio config
        audio_config_data = read_configuration(audio_config_path)
        if not audio_config_data:
            raise ValueError(f"Audio config data is invalid or missing: {audio_config_path}")

        # Monitor memory after loading user details
        monitor_memory_usage("After loading user details")

        # Initialize the GoogleSheetsManager
        sheets_manager = GoogleSheetsManager(service_account_key_path, google_sheet_id)

        # Ensure the client's sheet (tab) exists
        #sheets_manager.create_sheet_if_not_exists(container_specific_tab)

        # Clear the client's sheet (tab) before starting the job
        #sheets_manager.clear_sheet(container_specific_tab)

        # Use multiprocessing to process users in parallel
        with Manager() as manager:
            shared_user_details = manager.list(user_details)  # Shared list of users
            processed_users = manager.dict()  # Shared dict to track processed users
            video_details = []

            # Filter out already processed users before passing to the pool
            unprocessed_user_details = [user for user in shared_user_details if user['key'] not in processed_users]

            # Only proceed if there are unprocessed users
            if unprocessed_user_details:
                with Pool(2) as pool:  # Use all available CPU cores
                    worker_func = partial(generate_the_needful_for_users, 
                                          audio_config_data=audio_config_data, 
                                          customer_info_mapping=customer_info_mapping,
                                          sheets_manager=sheets_manager,
                                          container_specific_tab=container_specific_tab,
                                          processed_users=processed_users)

                    # Use map to block until all users are processed
                    results = pool.map(worker_func, unprocessed_user_details)

                    # Handle results, e.g., append valid video details to the list
                    for res in results:
                        if res:  # If a valid result was returned
                            video_details.append(res)
                        else:
                            logger.error("Failed to process a user")

        # Free memory by deleting user_details and shared_user_details after processing
        del user_details, shared_user_details
        monitor_memory_usage("After processing all users")

        logger.info("Process completed for all users. The END.")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
