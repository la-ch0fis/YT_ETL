import json
from datetime import date
import logging

# Define a logger object first. This is the preferred way of handling logging and it's a best practice to use this module and not the python "print" function.
# This is better for prod environments for output logs from Python function. The print function is good for a quick debug and for value printing
logger = logging.getLogger(__name__)

# Function that will be responsible of openning the JSON file, reading the data and parsing it into a Python object
def load_data():
    # Path where the JSON files are stored
    file_path = f"./data/YT_data_{date.today()}.json"
    try:
        logger.info(f"Processing file: YT_data_{date.today()}")
        # Get the data formt he JSON
        with open(file_path, 'r', encoding='utf-8') as raw_data:
            data = json.load(raw_data)
            # NOTE: If you're loading a huge file, maybe in the order of TB, you'll have memory issues becasue the entire file will be loaded in the memory
            # You'll need to probably stream the JSON using ijason library or procesing the filke line by line
        return data
    except FileNotFoundError:
        logger.error(f"File not found in path: {file_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON file in: {file_path}")
        raise



