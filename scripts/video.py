import os
import logging
from moviepy.editor import VideoFileClip, ColorClip, CompositeVideoClip, AudioFileClip, CompositeAudioClip
from utils import monitor_memory_usage, process_customer_data, get_text_clip, read_configuration, draw_animated_box
from config_loader import templates_folder, background_music_path, video_config_path, imagemagick_binary_path
import re
from moviepy.editor import AudioFileClip, CompositeAudioClip, CompositeVideoClip, VideoFileClip
import os

logger = logging.getLogger(__name__)

# Verify ImageMagick installation
from moviepy.config import change_settings
change_settings({"IMAGEMAGICK_BINARY": imagemagick_binary_path})

def generate_text_clips(config, video_duration, overlay_data, debug_mode=False):
    """
    Generates text clips for the video, positioned and timed according to the config and time marks.
    """
    text_clips = []
    default_font = "Arial-Bold"
    default_color = "#FB37B5"

    # Keep track of how many times each overlay has been processed
    
    try:
        for overlay in config['overlays']:
            mark_name = overlay['name']
            field_name = overlay.get('field_name', None)

            # Use the correct time mark or fallback to default time
        
            default_time_mark = overlay.get('default_time', 0)
            computed_start_time = default_time_mark

            # Determine the text duration based on the config or remaining video duration
            if 'show_till' in overlay:
                text_duration = overlay.get('show_till',0) - computed_start_time    
            elif 'duration' in overlay:
                text_duration = overlay.get('duration')
            else:
                text_duration = video_duration - computed_start_time

            #decide the text to show
            if 'text' in overlay:
                text_value = overlay['text']  # Static text, use directly
            elif field_name and field_name in overlay_data:
                text_value = overlay_data[field_name]  # Dynamic text from overlay_data
            else:
                logger.warning(f"No valid text or field_name found for overlay '{mark_name}'. Skipping.")
                continue  # Skip this overlay if neither 'text' nor 'field_name' is available


            box = overlay['dimensions']
            position = overlay['position']
            font = overlay.get('font', default_font)
            font_size = overlay.get('font_size', 120)
            color = overlay.get('color', default_color)

            try:
                # Generate text clip using `get_text_clip`
                text_clip = get_text_clip(text_value, position, font, font_size, box, color)
                if overlay.get('animated_box', {}).get('enabled', False):
                    animated_box = create_animated_box(overlay, computed_start_time, text_duration)
                    if animated_box:
                        text_clips.append(animated_box)
                        #logger.info(f"Animated box for '{mark_name}' created at {computed_start_time}")

                if text_clip:
                    # Set the start time and duration for the text clip
                    text_clip = text_clip.set_start(computed_start_time).set_duration(text_duration)
                    text_clips.append(text_clip)
                    text_clip.close()
                    #logger.info(f"Successfully created text clip for '{mark_name}' with text: '{text_value}' at time {computed_start_time}")
                else:
                    logger.warning(f"Failed to create text clip for '{mark_name}'.")

                # Add a debug box if in debug mode
                if debug_mode:
                    debug_box = ColorClip(size=(box['width'], box['height']), color=(255, 0, 0))
                    debug_box = debug_box.set_position((position['x'], position['y']))
                    debug_box = debug_box.set_opacity(0.3)  # Make the box semi-transparent
                    debug_box = debug_box.set_start(computed_start_time).set_duration(text_duration)
                    text_clips.append(debug_box)
                    #logger.info(f"Added debug box for '{mark_name}' at time {computed_start_time}.")

            except Exception as e:
                logger.error(f"Error processing overlay '{mark_name}': {e}")

        if not text_clips:
            logger.error(f"No valid text clips were created. Skipping video generation.")
            return None

        return text_clips

    except Exception as e:
        logger.error(f"Error generating text clips: {e}")
        return None

def generate_video(user_details, video_config, customer_info_mapping, audio_files, output_path, image_path):
    try:
        monitor_memory_usage(stage="before video generation")

        customer_data = user_details['mapping_data']

        # Read the configuration file
        if not video_config:
            raise ValueError(f"Configuration file {video_config_path} is empty or invalid.")
            
        # Determine the background video based on the template selection key (e.g., city, performance)
        background_clip, video_duration = get_background_clip(video_config, customer_data)
        processed_customer_data = process_customer_data(user_details['mapping_data'], customer_info_mapping, "video_processing")

        # Generate text clips
        text_clips = generate_text_clips(video_config, video_duration, processed_customer_data, False)
        if text_clips is None:
            raise ValueError(f"Failed to generate text clips for overlay data. Skipping video generation.")
        logger.info(f"Generated {len(text_clips)} text clips for {user_details['key']}")

        # Generate audio clips
        audio_clips = get_audio_clips(audio_files, video_config.get("audio_segments"))

        logger.info(f"Generated {len(audio_clips)} audio clips for {user_details['key']}")

        make_video(background_clip, audio_clips, text_clips, background_music_path, output_path, image_path, video_duration)

        logger.info(f"Video generation completed for {user_details['key']}, saved at {output_path}")
        return video_duration

    except Exception as e:
        logger.error(f"Error generating video for user with phone number {user_details.get('key', 'Unknown')}: {e}")
        return None
    finally:
        # Make sure all clips are released
        for clip in text_clips:
            clip.close()  # Close text clips explicitly
        for audio_clip in audio_clips:
            audio_clip.close()  # Close audio clips explicitly
        background_clip.close()  # Close the background clip
        monitor_memory_usage(stage="after video generation")

def get_audio_clips(audio_files, audio_segments):
    audio_clips = []
    
    # Convert audio_files to a dictionary with segment_name as key
    audio_files_dict = {file_data["segment_name"]: file_data for file_data in audio_files}
    
    for segment in audio_segments:
        segment_name = segment["segment_name"].strip()  # Ensure no extra spaces
        
        # Check if the segment exists in audio_files
        if segment_name in audio_files_dict:
            file_path = os.path.normpath(audio_files_dict[segment_name]["file"])  # Normalize path
                        
            # Load the audio file and set start time
            audio = AudioFileClip(file_path)
            audio = audio.set_start(segment["start_time"])
            audio_clips.append(audio)
        else:
            # Log or handle missing segment
            logger.error(f"Audio segment '{segment_name}' not found in audio_files.")
    
    return audio_clips

def get_background_clip(video_config, customer_data):
    """
    Determines and loads the background video based on the client's configuration and customer data.
    """
    # Determine the template selection key dynamically from config
    template_selection_key = video_config.get('template_selection_key', 'city')
    template_value = customer_data.get(template_selection_key, 'default')

    # Determine the background video path based on the template selection key
    background_path = os.path.join(
        templates_folder, video_config['backgrounds'][template_selection_key].get(template_value, video_config['backgrounds'][template_selection_key]['default'])
    )
    if not os.path.exists(background_path):
        background_path = os.path.join(templates_folder, video_config['backgrounds'][template_selection_key].get('default', 'default.mp4'))
        logger.warning(f"Template for {template_value} not found. Using default background: {background_path}")

    # Load the background video clip
    background_clip = VideoFileClip(background_path)
    video_duration = background_clip.duration
    return background_clip, video_duration

def create_animated_box(overlay, start_time, lifespan):
    """
    Creates an animated box around the overlay text.
    """
    box_settings = overlay['animated_box']
    box_color = tuple(box_settings.get('color', [255, 255, 255]))
    line_width = box_settings.get('line_width', 15)
    box_duration = min(box_settings.get('duration', 2), lifespan)  # Ensure box duration doesn't exceed text duration

    box_position = overlay['position']
    box_dimensions = overlay['dimensions']

    animated_box = draw_animated_box(
        box_position,
        box_dimensions,
        box_duration,
        start_time,
        lifespan,
        box_color=box_color,
        line_width=line_width
    )
    return animated_box

from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))  # Retry 3 times with exponential backoff
def write_video_with_retry(video, output_path):
    video.write_videofile(output_path, codec='libx264', fps=24, logger=None)

def make_video(background_clip, audio_clips, text_clips, background_music_path, output_path, image_path, video_duration):
    try:
        # Load the background music, set its volume, and subclip to the desired video duration
        background_music = AudioFileClip(background_music_path).subclip(0, video_duration).volumex(0.25).audio_fadeout(2)
        
        # Create a composite audio clip from all the audio clips including background music
        composite_audio = CompositeAudioClip([background_music] + audio_clips)
        
        # Add audio to the background video
        background_clip = background_clip.set_audio(composite_audio)
        
        # Create a composite video with text clips (assuming text_clips are video clips with text)
        video = CompositeVideoClip([background_clip] + text_clips)
        
        # Write the video to the output file
        #video.write_videofile(output_path, codec='libx264', fps=24, logger=None)
        
        # Write the video with retry logic
        write_video_with_retry(video, output_path)

        # Verify that the video was generated successfully
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            raise ValueError(f"Generated video file is invalid or corrupted: {output_path}")

        # Save a frame of the video for use as a thumbnail or reference image
        with VideoFileClip(output_path) as video_clip:
            frame_time = max(0, video_clip.duration - 2)
            video_clip.save_frame(image_path, t=frame_time)

        monitor_memory_usage(stage="after video generation")
        #logger.info(f"Generated video successfully saved at {output_path}")
    
    except Exception as e:
        logger.error(f"An error occurred during video generation: {str(e)}")
    
    finally:
        # Ensure all resources are released after the video is written
        video.close()  # Close the composite video clip
        for audio_clip in audio_clips:
            audio_clip.close()  # Close audio clips explicitly
        background_clip.close()  # Close the background clip explicitly
        composite_audio.close()  # Close composite audio explicitly
