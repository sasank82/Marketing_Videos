import os
import psutil
import logging
from dotenv import load_dotenv
from moviepy.editor import TextClip
import textwrap
import json
import re
from num2words import num2words
import unicodedata
import html
import re
from moviepy.editor import VideoClip
import numpy as np
from PIL import Image, ImageDraw

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Memory management function
def monitor_memory_usage(stage=""):
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss / (1024 * 1024)  # Convert to MB
    logger.info(f"Memory usage during {stage}: {memory_usage:.2f} MB")

# Format numbers in Indian style
def format_in_indian_style(number, include_currency=True):
    try:
        s = str(number)
        if len(s) > 3:
            last_three = s[-3:]
            rest = s[:-3]
            rest = rest[::-1]
            rest = ','.join(rest[i:i+2] for i in range(0, len(rest), 2))
            rest = rest[::-1]
            formatted_number = rest + ',' + last_three
        else:
            formatted_number = s
        return f"₹{formatted_number}" if include_currency else formatted_number
    except Exception as e:
        logger.error(f"Error formatting number {number}: {e}")
        return f"₹{number}" if include_currency else str(number)

# Get text clip for video
def get_text_clip(text, position, font, initial_font_size, box, color='white', max_lines=3):
    try:
        # Initial font size
        font_size = initial_font_size
        text_lines = [text]

        # Create initial TextClip without 'caption' method to avoid automatic wrapping
        clip = TextClip(text, fontsize=font_size, color=color, font=font)

        # Character limit per line based on box width
        char_limit = len(text) * box['width'] // clip.size[0]

        # Function to adjust line count and font size
        def adjust_text_lines_and_font():
            nonlocal font_size, text_lines, clip
            # Start with one line
            for lines in range(1, max_lines + 1):
                # Split text into multiple lines based on char limit and number of lines
                text_lines = textwrap.wrap(text, width=char_limit // lines)
                clip = TextClip("\n".join(text_lines), fontsize=font_size, color=color, font=font)

                # Check if it fits both horizontally and vertically
                if clip.size[0] <= box['width'] and clip.size[1] <= box['height']:
                    break

        # Try fitting with larger font and multiple lines
        adjust_text_lines_and_font()

        # If the text still doesn't fit, reduce the font size
        while (clip.size[0] > box['width'] or clip.size[1] > box['height']) and font_size > 25:
            font_size -= 5
            adjust_text_lines_and_font()

        # Calculate the position within the box (centered)
        box_top_left_x = position['x']
        box_top_left_y = position['y']
        text_width, text_height = clip.size
        x = box_top_left_x + (box['width'] - text_width) / 2
        y = box_top_left_y + (box['height'] - text_height) / 2

        return clip.set_position((x, y))
    except Exception as e:
        logger.error(f"Error creating text clip for text '{text}': {e}")
        return None        

def sanitize_text(text):
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize Unicode characters
    text = unicodedata.normalize('NFKC', text)
    # Remove non-printable characters
    sanitized_text = ''.join(c for c in text if c.isprintable())
    # Replace any sequence of whitespace characters with a single space
    sanitized_text = re.sub(r'\s+', ' ', sanitized_text)
    sanitized_text = sanitized_text.strip()
    sanitized_text = sanitized_text.title()
    return sanitized_text

def read_configuration(config_path):
    """
    Reads a configuration JSON file and returns its content as a dictionary.
    """
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding="utf-8") as file:
                return json.load(file)
        else:
            logger.error(f"Configuration file {config_path} does not exist.")
            return {}
    except Exception as e:
        logger.error(f"Error reading configuration file {config_path}: {e}")
        return {}

def process_names(name_column):
    names = []
    for name in name_column.split('|'):
        name = name.strip()
        if not name:
            continue  # Skip empty names
        name_parts = name.split()
        if not name_parts:
            continue  # Skip if no name parts
        first_name = name_parts[0].title()
        names.append(first_name)
    if len(names) == 0:
        return ''
    elif len(names) == 1:
        return names[0]
    elif len(names) == 2:
        return f"{names[0]} and {names[1]}"
    else:
        return ", ".join(names[:-1]) + f", and {names[-1]}"

def process_names_respect(name_column):
    names = []
    for name in name_column.split('|'):
        name = name.strip()
        if not name:
            continue  # Skip empty names
        name_parts = name.split()
        if not name_parts:
            continue  # Skip if no name parts
        first_name = name_parts[0].title()
        names.append(first_name)
    if len(names) == 0:
        return ''
    elif len(names) == 1:
        return f"{names[0]} Ji"
    elif len(names) == 2:
        return f"{names[0]} and {names[1]} Ji"
    else:
        return ", ".join(names[:-1]) + f", and {names[-1]} Ji"


def draw_animated_box(position, dimensions, box_draw_duration, start_time, lifespan, box_color=(255, 255, 255), line_width=15):
    """
    Creates an animated effect where a transparent box is drawn around the given position and dimensions.
    """
    try:
        box_width = dimensions['width'] + 30
        box_height = dimensions['height'] + 30
        x_pos = position['x'] - 15
        y_pos = position['y'] - 15

        def make_frame(t):
            # Create a fully transparent RGBA image
            img = Image.new('RGBA', (box_width, box_height), (0, 0, 0, 0))  
            draw = ImageDraw.Draw(img)
            progress = t / box_draw_duration  # Progress of the drawing animation (0 to 1)

            # Drawing the box progressively
            if progress < 0.25:
                line_length = box_width * (progress / 0.25)
                draw.line([(0, 0), (line_length, 0)], fill=box_color, width=line_width)
            elif progress < 0.5:
                draw.line([(0, 0), (box_width, 0)], fill=box_color, width=line_width)  
                line_length = box_height * ((progress - 0.25) / 0.25)
                draw.line([(box_width, 0), (box_width, line_length)], fill=box_color, width=line_width)
            elif progress < 0.75:
                draw.line([(0, 0), (box_width, 0)], fill=box_color, width=line_width)
                draw.line([(box_width, 0), (box_width, box_height)], fill=box_color, width=line_width)  
                line_length = box_width * ((progress - 0.5) / 0.25)
                draw.line([(box_width, box_height), (box_width - line_length, box_height)], fill=box_color, width=line_width)
            else:
                draw.line([(0, 0), (box_width, 0)], fill=box_color, width=line_width)  
                draw.line([(box_width, 0), (box_width, box_height)], fill=box_color, width=line_width)
                draw.line([(box_width, box_height), (0, box_height)], fill=box_color, width=line_width)  
                line_length = box_height * ((progress - 0.75) / 0.25)
                draw.line([(0, box_height), (0, box_height - line_length)], fill=box_color, width=line_width)

            # Split the image into RGB and Alpha (transparency)
            r, g, b, a = img.split()

            # Convert the image to RGB for MoviePy compatibility
            img_rgb = Image.merge('RGB', (r, g, b))

            # Convert the image and alpha channel (mask) to numpy arrays
            img_array = np.array(img_rgb)
            mask_array = np.array(a) / 255.0  # Normalize the alpha channel

            return img_array, mask_array

        # Create the animated box clip
        animated_box = VideoClip(lambda t: make_frame(t)[0], duration=box_draw_duration).set_position((x_pos, y_pos))

        # Add the transparency mask to the VideoClip and ensure it's a valid mask by setting ismask=True
        mask_clip = VideoClip(lambda t: make_frame(t)[1], duration=box_draw_duration).set_position((x_pos, y_pos)).set_ismask(True)

        # Attach the mask to the animated box to retain transparency
        animated_box = animated_box.set_mask(mask_clip)

        # Extend the box visibility for the rest of the video duration
        animated_box = animated_box.set_duration(lifespan).set_start(start_time)

        return animated_box

    except Exception as e:
        logger.error(f"Error creating animated box draw: {e}")
        return None

def process_customer_data(customer_data, customer_info_mapping, mode):
    """
    Processes the customer_info data based on the 'processing' rules in customer_info_mapping.
    Handles name sanitization, ordinal formatting, float rounding, etc.
    """
    def get_ordinal_suffix(number):
        if 10 <= number % 100 <= 20:
            return f"{number}th"
        else:
            return f"{number}{['th', 'st', 'nd', 'rd', 'th'][min(number % 10, 4)]}"

    def remove_ordinal_suffix(value):
        """
        Removes the ordinal suffix from a value if it's already present (e.g., '15th' -> '15').
        """
        return ''.join(filter(str.isdigit, str(value)))  # Keeps only the digits, removing the 'th', 'st', etc.

    def clean_percentage(value):
        """
        Removes the '%' sign from a string and converts the result to a float.
        If the value is already a float, it returns the value as is.
        """
        if isinstance(value, str):
            return float(value.strip('%'))  # Remove any '%' sign and convert to float
        return float(value)  # If already float, just return the value

    processed_data = {}
    for element, value in customer_data.items():
        processing_type = customer_info_mapping.get(element, {}).get(mode, 'none')

        if processing_type == 'name':
            processed_data[element] = process_names(value)
        elif processing_type == 'name_respect':
            processed_data[element] = process_names_respect(value)        
        elif processing_type == 'ordinal':
            cleaned_value = remove_ordinal_suffix(value)
            processed_data[element] = num2words(cleaned_value, to='ordinal')
        elif processing_type == 'float':
            round_to = customer_info_mapping.get(element, {}).get('round_to', 2)
            processed_data[element] = f"{float(value):.{round_to}f}"
        elif processing_type == 'percentage_readout':
            round_to = customer_info_mapping.get(element, {}).get('round_to', 0)
            cleaned_value = clean_percentage(value)  # Clean '%' before processing
            processed_data[element] = f"{num2words(round(cleaned_value, round_to))} percent"
        elif processing_type == 'percentile_readout':
            round_to = customer_info_mapping.get(element, {}).get('round_to', 0)
            cleaned_value = clean_percentage(value)  # Clean '%' before processing
            processed_data[element] = f"{num2words(round(cleaned_value, round_to))} percentile"
        elif processing_type == 'integer':
            processed_data[element] = str(value)
        elif processing_type == 'percentage':
            round_to = customer_info_mapping.get(element, {}).get('round_to', 0)
            processed_data[element]=f"{round(value,round_to)}%" 
        elif processing_type == 'percentile':
            round_to = customer_info_mapping.get(element, {}).get('round_to', 0)
            processed_data[element]=f"{round(value,round_to)}%ile"     
        else:
            if isinstance(value, int):
                processed_data[element] = str(value)
            else:
                processed_data[element] = sanitize_text(value)  # Default to sanitizing text for safety
    return processed_data
