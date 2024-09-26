import os
import logging
import time
from google.cloud import texttospeech_v1beta1 as texttospeech
from utils import process_customer_data, monitor_memory_usage
from config_loader import voiceovers_dir, service_account_key_path
import re
import json

logger = logging.getLogger(__name__)

def generate_voiceover_script(user_details, customer_info_mapping, audio_segments):
    """
    Generates the SSML script for the voiceover based on the user details and customer info mapping
    The template is a JSON file with placeholders for customer data.
    This version returns the updated JSON structure with populated placeholders.
    """
    try:
        #monitor_memory_usage("After reading voiceover template")

        # Process customer data based on the mapping for audio processing
        processed_customer_data = process_customer_data(user_details['mapping_data'], customer_info_mapping, "audio_processing")

        # Iterate through each audio segment and format the 'speech_text'
        for segment in audio_segments:
            speech_text = segment['speech_text']
            
            # Replace placeholders in 'speech_text' with actual values from processed_customer_data
            for placeholder, value in processed_customer_data.items():
                speech_text = speech_text.replace(f'{{{placeholder}}}', str(value))
            
            # Update the 'speech_text' in the template with formatted SSML
            segment['speech_text'] = speech_text

        #monitor_memory_usage("After generating SSML script")

        # Return the modified template as a JSON object
        logger.info(f"Generated SSML for {len(audio_segments)} segments for: {user_details['key']}")
        return audio_segments

    except (FileNotFoundError, IOError) as file_error:
        logger.error(f"File error: {file_error}")
        raise

    except Exception as e:
        logger.error(f"Error in generating voiceover script: {e}")
        raise


def generate_audio_files(audio_segments, audio_config, user_details, voiceovers_dir=voiceovers_dir):
    try:
        # Placeholder for combined audio files, time marks, and synthesis times
        combined_audio_data = []
        total_synthesis_time = 0

        # Iterate through each audio segment in the JSON
        for idx, segment in enumerate(audio_segments):
            segment_name = segment['segment_name']
            segment_script = segment['speech_text']
                      
            # Call the function to generate audio for each segment
            audio_content, time_marks, synthesis_time = generate_audio_content(
                segment_script,
                audio_config,
                user_details
            )
            
            # Define the file path for the generated audio
            audio_file_path = os.path.join(voiceovers_dir, user_details['key'], f"audio_part_{idx + 1}.mp3")
            
            # Ensure the directory exists
            os.makedirs(os.path.dirname(audio_file_path), exist_ok=True)

            # Save the audio content to the specified file path
            with open(audio_file_path, 'wb') as audio_file:
                audio_file.write(audio_content)
            
            # Store the information for this segment
            combined_audio_data.append({
                "segment_name": segment_name,
                "file": audio_file_path,
                "time_marks": time_marks,
                "synthesis_time": synthesis_time
            })
            
            # Update total synthesis time
            total_synthesis_time += synthesis_time
        
        logger.info(f"All audio segments processed successfully for user {user_details['key']}.")

        # Return the combined audio data
        return combined_audio_data, total_synthesis_time

    except Exception as e:
        logger.error(f"Error during processing segments for {user_details['key']}: {e}")
        return [], 0

def generate_audio_content(script, audio_config, user_details):
    try:
        # Memory profiling before audio synthesis
        #monitor_memory_usage("Before audio synthesis")

        # Initialize Google TTS client
        client = texttospeech.TextToSpeechClient.from_service_account_file(service_account_key_path)

        # Configure voice properties
        voice = texttospeech.VoiceSelectionParams(
            name=audio_config['voice_name'],
            ssml_gender=texttospeech.SsmlVoiceGender.MALE,
            language_code=audio_config['language_code']
        )

        # Configure audio parameters
        audio_config_params = texttospeech.AudioConfig(
            audio_encoding=getattr(texttospeech.AudioEncoding, audio_config['audio_encoding']),
            pitch=audio_config['pitch'],
            volume_gain_db=audio_config['volume_gain_db'],
            speaking_rate=audio_config['speaking_rate'],
            sample_rate_hertz=audio_config['sample_rate_hertz'],
            effects_profile_id=[audio_config['effects_profile_id']]
        )

        # Prepare SSML synthesis input
        synthesis_input = texttospeech.SynthesisInput(ssml=script)
        request = texttospeech.SynthesizeSpeechRequest(
            input=synthesis_input,
            voice=voice,
            audio_config=audio_config_params,
            enable_time_pointing=[texttospeech.SynthesizeSpeechRequest.TimepointType.SSML_MARK]
        )

        # Measure synthesis time
        start_time = time.time()
        response = client.synthesize_speech(request=request)
        synthesis_time = time.time() - start_time
        #logger.info(f"Synthesis time: {synthesis_time:.2f} seconds")

        audio_content = response.audio_content

        # Process time marks
        time_marks = {}
        for tp in response.timepoints:
            if hasattr(tp, 'time_seconds'):
                time_marks[tp.mark_name] = tp.time_seconds
            else:
                logger.warning(f"Invalid timepoint for mark {tp.mark_name} for user {user_details['key']}")

        # Memory profiling after audio synthesis
        #monitor_memory_usage("After audio synthesis")

        # Memory Management: Free up resources explicitly
        del client, synthesis_input, request, response  # Release resources after processing
        #monitor_memory_usage("After resource cleanup")

        return audio_content, time_marks, synthesis_time

    except Exception as e:
        logger.error(f"Error during audio file generation for {user_details['key']}: {e}")
        return None, {}, 0
