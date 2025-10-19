# main.py

"""
This script serves as the main entry point for running the entire ETL pipeline.

It orchestrates the process by calling the main functions from the extract,
transform, and load modules in the correct sequence. This allows the entire
pipeline to be executed from a single command.
"""

import logging
from sqlalchemy import create_engine, exc
from dotenv import load_dotenv
import os
from extract import get_weather_data, read_csv_data, scrape_book_listings
from transform import transform_data
from load import setup_database, load_data_to_db

# Configure the logging system ---
logging.basicConfig(
    level=logging.INFO,  # Set the minimum level of messages to log (e.g., INFO, DEBUG, WARNING)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the format of the log messages
    datefmt='%Y-%m-%d %H:%M:%S'  # Define the format of the timestamp
)

# Define constants for the pipeline's configuration
DB_PATH = "data/data.db"
TABLE_NAME = "unified_data"
CSV_PATH = "data/sales_data.csv"

load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
WEATHER_CITY = os.getenv("WEATHER_CITY", "SAN FRANCISCO")
SCRAPE_URL = os.getenv("SCRAPE_URL", "http://books.toscrape.com/")

def run_pipeline():
    """
    Executes the full ETL (Extract, Transform, Load) pipeline.
    """
    # --- HIGHLIGHTED CHANGES: Replaced all print() with logging.info() or logging.error() ---
    logging.info("ETL Pipeline Started...")

    try:
        # --- 1. EXTRACT PHASE ---
        logging.info("Phase 1: Extracting data...")
        if OPENWEATHER_API_KEY:
           api_data = get_weather_data(OPENWEATHER_API_KEY, WEATHER_CITY)
        else:
           logging.warning("OPENWEATHER_API_KEY not set; continuing without weather data.")
           api_data = None
        
        csv_data = read_csv_data(CSV_PATH)
        scraped_data = scrape_book_listings(SCRAPE_URL)
        logging.info("Extraction complete.")

        # --- 2. TRANSFORM PHASE ---
        logging.info("Phase 2: Transforming data...")
        unified_df = transform_data(
             api_data,
             csv_data,
             scraped_data
        )
        logging.info("Transformation complete. Unified DataFrame created.")

        # --- 3. LOAD PHASE ---
        logging.info("Phase 3: Loading data to database...")
        db_uri = f"sqlite:///{DB_PATH}"
        engine = create_engine(db_uri)
        setup_database(DB_PATH)
        load_data_to_db(unified_df, engine, TABLE_NAME)
        logging.info("Loading complete.")
        logging.info("ETL Pipeline Finished Successfully.")

    except FileNotFoundError as e:
        # Log specific, expected errors with a clear message.
        logging.error(f"Error: A required file was not found. {e}")
    except exc.SQLAlchemyError as e:
        # Log database-specific errors.
        logging.error(f"Error: A database error occurred. {e}")
    except Exception as e:
        # For any other unexpected error, log a critical message and include the traceback.
        # The exc_info=True parameter automatically adds exception information.
        logging.critical(f"An unexpected error occurred during the pipeline execution.", exc_info=True)
    # --- END HIGHLIGHTED CHANGES ---

if __name__ == '__main__':
    run_pipeline()
