import logging
import os
import re
from utils import monitor_memory_usage
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_customer_info(customer_info_sheet, customer_info_mapping, start_row, end_row):
    """
    Reads customer information from an Excel sheet and processes it based on the mapping configuration.
    
    Args:
        customer_info_sheet (str): Path to the Excel file containing customer information.
        customer_info_mapping (dict): Mapping configuration for the fields in the Excel sheet.
    
    Returns:
        list: A list of dictionaries, each containing 'phone_number' and 'mapping_data'.
    """
    try:      
        #Read the mapping sheet to identify primary field name
        def find_primary_field(data):
            primary_field = None
    
            # Iterate over the fields in the data
            for field, attributes in data.items():
                # Check if "IsPrimary" is set to True
                if attributes.get("IsPrimary") == "True":  # Using string since JSON is often loaded with string values
                    if primary_field is not None:
                        # If we already found a primary field, raise an error
                        raise ValueError(f"Multiple primary fields detected: '{primary_field}' and '{field}'")
                    primary_field = field
    
            if primary_field is None:
                raise ValueError("No primary field found.")
    
            return primary_field

        # Load the Excel file
        primary_field = find_primary_field(customer_info_mapping)

        try:
            df = pd.read_excel(customer_info_sheet, skiprows=range(1,start_row), nrows=end_row-start_row + 1)
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}")
        except KeyError as e:
            logger.error(f"Missing key in Excel file: {e}")

        if df.empty:
            raise ValueError(f"Excel file {customer_info_sheet} is empty or invalid.")
        
        user_details = []
              
        # Check if the primary field exists in the data
        if primary_field not in df.columns:
            raise KeyError(f"Phone number column '{primary_field}' not found in the Excel sheet.")

        # Iterate through each row and process the data
        for index, row in df.iterrows():
            mapping_data = {}
            
            for element, mapping in customer_info_mapping.items():
                column_name = mapping.get('column_name')
                

                if column_name not in df.columns:
                    logger.warning(f"Column '{column_name}' for field '{element}' not found in row {index}. Skipping this field.")
                    continue  # Skip missing fields
                
                value = row.get(column_name)
                if pd.isna(value):
                    logger.warning(f"Missing value for column '{column_name}' in row {index}. Skipping this field.")
                    continue
                
                mapping_data[element] = value
                
            # Retrieve and clean phone number
            #phone_number = re.sub(r'\D', '', str(row.get(phone_number_column_name, None)))
            primary_value= str(row.get(primary_field, None))
            if not primary_value:
                logger.warning(f"Primary key missing or invalid for row {index}. Skipping this row.")
                continue  # Skip rows with missing/invalid phone numbers

            user_details.append({
                'key': primary_value,
                'mapping_data': mapping_data,
            })
            logger.info(f"Processed user details for: {primary_value}")
        
        # Release the DataFrame memory once it's no longer needed
        del df
        
        #monitor_memory_usage("After reading user details")
        
        return user_details

    except Exception as e:
        logger.exception(f"Error in processing customer info: {e}")
        return []
