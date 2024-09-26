import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Get the client name and other variables from the environment
client_name = os.getenv('CLIENT_NAME')

# Dynamic paths for each client's resources
service_account_key_path = os.getenv('SERVICE_ACCOUNT_KEY_PATH')
google_sheet_id = os.getenv('GOOGLE_SHEET_ID')
voiceover_bucket_name = os.getenv("VOICEOVER_BUCKET_NAME")
video_bucket_name = os.getenv("VIDEO_BUCKET_NAME")
cover_image_bucket_name = os.getenv("COVER_IMAGE_BUCKET_NAME")

# Paths for configuration files and client-specific resources
audio_config_path = os.getenv('AUDIO_CONFIG_PATH')
video_config_path = os.getenv('VIDEO_CONFIG_PATH')
customer_info_sheet = os.getenv('CUSTOMER_INFO_SHEET_PATH')
background_music_path = os.getenv('BACKGROUND_MUSIC_PATH')
templates_folder = os.getenv('TEMPLATES_FOLDER')

# Path for overlay mapping
customer_info_mapping_path = os.getenv('CUSTOMER_INFO_MAPPING_PATH')

# Path for voiceovers should be dynamically picked up
voiceovers_dir = os.getenv('VOICEOVERS_DIR')
videos_dir = os.getenv('VIDEOS_DIR')
cover_images_dir = os.getenv('COVER_IMAGES_DIR')

# Max Users
max_users = int(os.getenv('MAX_USERS', 2))  # Set a default value like 10 if needed

# Image Magick Binary
imagemagick_binary_path = os.getenv("IMAGEMAGICK_BINARY_PATH")

# Check if paths exist
required_paths = [
    service_account_key_path,
    audio_config_path,
    video_config_path,
    customer_info_sheet,
    background_music_path,
    templates_folder
]

for path in required_paths:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file or folder not found: {path}")

if not os.path.exists(voiceovers_dir):
    os.makedirs(voiceovers_dir, exist_ok=True)
    logger.info(f"Voiceovers directory created at {voiceovers_dir}")

if not os.path.exists(videos_dir):
    os.makedirs(videos_dir, exist_ok=True)
    logger.info(f"Videos directory created at {videos_dir}")

if not os.path.exists(cover_images_dir):
    os.makedirs(cover_images_dir, exist_ok=True)
    logger.info(f"Cover Images directory created at {cover_images_dir}")
