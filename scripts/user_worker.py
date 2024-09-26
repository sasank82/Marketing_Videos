import logging
import os
from voiceover import generate_voiceover_script, generate_audio_files
from video import generate_video
from gcs_utils import upload_to_gcs, GoogleSheetsManager
from config_loader import video_config_path, videos_dir, cover_images_dir, cover_image_bucket_name, video_bucket_name, service_account_key_path, google_sheet_id, client_name
from utils import monitor_memory_usage, read_configuration
import time
import multiprocessing

logger = logging.getLogger(__name__)

def generate_the_needful_for_users(user_details, audio_config_data, customer_info_mapping, sheets_manager, container_specific_tab, processed_users):
    video_details = {}
    #lock = multiprocessing.Lock()

    user_key = user_details['key']

    # Step 0: Check if this user has already been processed
    if user_key in processed_users:
        logger.info(f"Skipping user {user_key}, already processed.")
        return None

    # Step 1: Read video configuration
    try:
        video_config = read_configuration(video_config_path)
    except Exception as e:
        logger.error(f"Error reading video configuration for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Video config read failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Step 2: Generate the voiceover script
    try:
        audio_segments = generate_voiceover_script(user_details, customer_info_mapping, video_config.get("audio_segments"))
        if not audio_segments:
            raise ValueError("Voiceover script generation failed")
    except Exception as e:
        logger.error(f"Voiceover script generation failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Voiceover script failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Step 3: Generate the audio file (with time marks)
    try:
        audio_files, synthesis_time = generate_audio_files(audio_segments, audio_config_data, user_details)
        if not audio_files:
            raise ValueError("Audio file generation failed")
    except Exception as e:
        logger.error(f"Audio file generation failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Audio file generation failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Memory Profiling
    monitor_memory_usage(f"After generating audio for {user_details['key']}")
                 
    # Step 4: Ensure the output directories exist for videos and cover images
    try:
        if not os.path.exists(videos_dir):
            os.makedirs(videos_dir, exist_ok=True)

        if not os.path.exists(cover_images_dir):
            os.makedirs(cover_images_dir, exist_ok=True)
    except Exception as e:
        logger.error(f"Directory creation failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Directory creation failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Step 5: Generate video based on time marks and voiceover path
    video_filename = f"{user_details['key']}.mp4"
    output_path = os.path.join(videos_dir, video_filename)
    image_path = os.path.join(cover_images_dir, video_filename.replace(".mp4", ".jpg"))

    try:
        video_duration = generate_video(user_details, video_config, customer_info_mapping, audio_files, output_path, image_path)
        if not video_duration:
            raise ValueError("Video generation failed")
    except Exception as e:
        logger.error(f"Video generation failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Video generation failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Memory Profiling after video generation
    monitor_memory_usage(f"After generating video for {user_details['key']}")

    # Step 6: Upload the cover image to GCS
    try:
        cover_image_gcs_url = upload_to_gcs(cover_image_bucket_name, image_path)
        if not cover_image_gcs_url:
            raise ValueError("Cover image upload failed")
        logger.info(f"Cover image uploaded to GCS at {cover_image_gcs_url}")
    except Exception as e:
        logger.error(f"Cover image upload failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Cover image upload failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Step 7: Upload the video to GCS
    try:
        video_gcs_url = upload_to_gcs(video_bucket_name, output_path)
        if not video_gcs_url:
            raise ValueError("Video upload failed")
        logger.info(f"Video uploaded to GCS at {video_gcs_url}")
    except Exception as e:
        logger.error(f"Video upload failed for user {user_details['key']}: {e}")
        #sheets_manager.log_failure(user_details['key'], f"Video upload failure: {str(e)}", sheet_name="Failures")
        return video_details

    # Step 8: Log successful operations to Google Sheets
    #try:
    #    with lock:
    #        sheets_manager.log_to_sheet([
    #            user_details['key'], video_gcs_url, cover_image_gcs_url, video_duration
    #        ], container_specific_tab)
    #        #logger.info(f"Successfully logged {user_details['key']} to Google Sheets")
    #    time.sleep(1)  # Delay to avoid potential write conflicts
    #except Exception as e:
    #    logger.error(f"Failed to log {user_details['key']} to Google Sheets: {e}")
    #    sheets_manager.log_failure(user_details['key'], f"Google Sheets logging failure: {str(e)}", sheet_name="Failures")
    #    return video_details

    # Step 9: Mark the user as processed in the shared directory
    processed_users[user_key] = True

    # Construct video details to return
    video_details = {
        'key': user_details['key'],
        'voiceover_gcs_path': '',  # If needed, you could return this as well.
        'video_gcs_path': video_gcs_url,
        'cover_image_gcs_path': cover_image_gcs_url,
        'video_duration': video_duration
    }

    # Memory Profiling at the end
    monitor_memory_usage(f"Completed video generation for {user_details['key']}")

    return video_details
