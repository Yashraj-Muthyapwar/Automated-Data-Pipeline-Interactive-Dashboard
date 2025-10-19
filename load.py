# load.py

import sqlite3
from sqlite3 import Error
import pandas as pd
from sqlalchemy import create_engine

"""
This script will contain all the functions responsible for loading the
transformed data into the final destination: a SQLite database.
"""

def create_db_connection(db_path):
    """
    Creates a persistent connection to a SQLite database.
    Creates the database file if it does not exist.

    Args:
        db_path (str): The file path for the SQLite database.

    Returns:
        sqlite3.Connection or None: A connection object to the database,
                                    or None if a connection could not be established.
    """
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        print(f"Successfully connected to SQLite database at '{db_path}'")
        return conn
    except Error as e:
        print(f"Error: Could not connect to the database at '{db_path}'.")
        print(e)
        return None

# --- NEW SQL STATEMENT DEFINITION ---
def create_table(conn):
    """
    Creates a table from the create_table_sql statement.

    Args:
        conn (sqlite3.Connection): Connection object to the database.
    """

    # This multi-line string contains the SQL command to create our table.
    # It defines the table name, each column, and the data type for each column.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS unified_data (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        sale_date TEXT,
        sale_amount REAL,
        sale_month INTEGER,
        sale_day_name TEXT,
        city TEXT,
        temperature_celsius REAL,
        weather_condition TEXT,
        humidity_percent REAL,
        wind_speed_m_s REAL,
        report_timestamp TEXT,
        book_title TEXT,
        book_availability TEXT,
        book_price_gbp REAL
    );
    """
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        print("Table 'unified_data' created successfully (if it didn't already exist).")
    except Error as e:
        # This block will catch any errors that occur during the SQL execution.
        print("Error: Could not create the table.")
        print(e)

def setup_database(db_path):
    """
    Establishes a database connection and creates the necessary table.
    This function remains as our setup utility using the low-level sqlite3 driver.

    Args:
        db_path (str): The file path for the SQLite database.
    
    Returns:
        bool: True if setup was successful, False otherwise.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS unified_data (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        sale_date TEXT,
        sale_amount REAL,
        sale_month INTEGER,
        sale_day_name TEXT,
        city TEXT,
        temperature_celsius REAL,
        weather_condition TEXT,
        humidity_percent REAL,
        wind_speed_m_s REAL,
        report_timestamp TEXT,
        book_title TEXT,
        book_availability TEXT,
        book_price_gbp REAL
    );
    """
    try:
        with sqlite3.connect(db_path) as conn:
            print(f"Successfully connected to SQLite database at '{db_path}' for setup.")
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            print("Table 'unified_data' is ready.")
        return True
            
    except Error as e:
        print(f"Error: Could not set up the database at '{db_path}'.")
        print(e)
        return False

def load_data_to_db(dataframe, engine, table_name):
    """
    Loads a DataFrame into a specific table in the SQLite database
    using the provided SQLAlchemy engine.

    Args:
        dataframe (pd.DataFrame): The cleaned DataFrame to be loaded.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database connection.
        table_name (str): The name of the table to load data into.
    """
    # The engine is now passed in, not created here.
    # This function's single job is to handle the loading.
    
    if dataframe.empty:
        print("The provided DataFrame is empty. No data will be loaded.")
        return

    num_rows = len(dataframe)
    print(f"Preparing to load {num_rows} rows into table '{table_name}'...")

    try:
        # This is the core command for loading the data.
        dataframe.to_sql(
            name=table_name,       # The name of the SQL table.
            con=engine,            # The SQLAlchemy engine connection.
            if_exists='append',   # What to do if the table already exists.
            index=False            # Do not write the DataFrame index as a column.
)        
        print(f"Successfully loaded {num_rows} rows into '{table_name}'.")

    except (exc.SQLAlchemyError, ValueError) as e:
        # Catch specific SQLAlchemy errors or ValueError (e.g., from data type issues)
        print(f"An error occurred during the data loading process: {e}")    

if __name__ == '__main__':
    DB_FILE_PATH = "data/data.db"
    TABLE_NAME = "unified_data"

    print("--- Setting up database ---")
    success = setup_database(DB_FILE_PATH)
    
    if success:
        print("Database setup completed successfully.")
        print("\n--- Creating SQLAlchemy Engine ---")
        db_uri = f"sqlite:///{DB_FILE_PATH}"
        try:
            engine = create_engine(db_uri)
            print("SQLAlchemy engine created successfully.")
            
            # 3. Create a dummy DataFrame and call the refactored loading function.
            print("\n--- Testing data loading function ---")
            dummy_df = pd.DataFrame({
                'product_id': [999], 'sale_date': ['2023-01-01'], 'sale_amount': [100.0],
                'sale_month': [1], 'sale_day_name': ['Sunday'], 'city': ['Testville'],
                'temperature_celsius': [10.0], 'weather_condition': ['Clear'],
                'humidity_percent': [50.0], 'wind_speed_m_s': [2.5],
                'report_timestamp': ['2023-01-01 12:00:00'], 'book_title': ['Test Book'],
                'book_availability': ['In stock'], 'book_price_gbp': [9.99]
            })
            
            # Pass the dataframe, engine, and table name to the function.
            load_data_to_db(dummy_df, engine, TABLE_NAME)

        except exc.SQLAlchemyError as e:
            print(f"An error occurred with the SQLAlchemy engine: {e}")
    else:
        print("Database setup failed. Please check the errors above.") 
