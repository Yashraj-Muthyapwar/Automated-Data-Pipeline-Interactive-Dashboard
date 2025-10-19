# dashboard.py

"""
This script creates a web-based dashboard using Streamlit to visualize
the data from the 'unified_data' table in our SQLite database.

The dashboard will allow users to view the raw data, see summary charts,
and apply filters to explore the data interactively.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, exc

# Define a constant for the database path.
DB_PATH = "data/data.db"

def get_db_engine(db_path):
    """
    Creates and returns a SQLAlchemy engine for the specified SQLite database path.

    Args:
        db_path (str): The file path for the SQLite database.

    Returns:
        sqlalchemy.engine.Engine or None: The SQLAlchemy engine if connection is successful,
                                          otherwise None.
    """
    db_uri = f"sqlite:///{db_path}"
    try:
        engine = create_engine(db_uri)
        return engine
    except exc.SQLAlchemyError as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# --- NEW: Function to load data from the database ---
def load_data_from_db(engine):
    """
    Loads data from the 'unified_data' table into a Pandas DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The SQLAlchemy engine for the database connection.

    Returns:
        pd.DataFrame or None: A DataFrame containing the data if successful, otherwise None.
    """
    try:
        # Define the SQL query to select all data from the table.
        query = "SELECT * FROM unified_data"
        
        # Use pandas.read_sql to execute the query and load the data into a DataFrame.
        # This single function handles the connection, query execution, and data loading.
        df = pd.read_sql(query, engine)
        
        # A good practice is to convert date columns to datetime objects after loading.
        # This enables time-based filtering and charting.
        df['sale_date'] = pd.to_datetime(df['sale_date'])
        df['report_timestamp'] = pd.to_datetime(df['report_timestamp'])
        
        return df
    except (exc.SQLAlchemyError, ValueError) as e:
        # If the table doesn't exist or another DB error occurs, show a warning.
        st.warning(f"Could not load data from the database. Error: {e}")
        return None

def main():
    """
    The main function that sets up and runs the Streamlit dashboard.
    """
    st.title("Automated Data Pipeline Dashboard")
    
    engine = get_db_engine(DB_PATH)

    if engine:
        df = load_data_from_db(engine)

        if df is not None and not df.empty:
            
            # --- SIDEBAR AND FILTER LOGIC STARTS HERE ---
            
            st.sidebar.header("Dashboard Filters")
            st.sidebar.markdown("""
            Use the filters below to slice and dice the data. The charts and table
            on the main page will update automatically.
            """)

            # Create the multi-select widget for cities.
            all_cities = df['city'].unique().tolist()
            selected_cities = st.sidebar.multiselect(
                label="Filter by City:",
                options=all_cities,
                default=all_cities  # By default, all cities are selected.
            )

            # 1. Start with a copy of the full dataframe.
            df_filtered = df.copy()       
   
            if selected_cities:
                df_filtered = df_filtered[df_filtered['city'].isin(selected_cities)]
            else:
                st.warning("No cities selected. Please select at least one city from the sidebar.")
                df_filtered = pd.DataFrame() # Create an empty DataFrame

            # --- MAIN PAGE CONTENT (NOW USES THE FILTERED DATAFRAME) ---
            if not df_filtered.empty:
                st.header("Unified Data View")
                st.markdown(
                f"Displaying data for **{len(df_filtered)}** records. You can sort by clicking on a column header."
            )
            # 3. DISPLAY THE FILTERED DATA: Use df_filtered from now on.
                st.dataframe(df_filtered)
            
            # --- Time-Series Visualization Section ---
                st.header("Sales Trend Over Time")
                st.markdown("This line chart shows the total sales amount per day for the selected cities.")
            
            # Aggregate the FILTERED data for the line chart.
                daily_sales = df_filtered.groupby('sale_date')['sale_amount'].sum()
                st.line_chart(daily_sales)
            
            # --- Categorical Visualization Section ---
                st.header("Sales Performance by City")
                st.markdown("This bar chart shows the total sales amount for each selected city.")

            # Aggregate the FILTERED data for the bar chart.
                sales_by_city = df_filtered.groupby('city')['sale_amount'].sum()
                st.bar_chart(sales_by_city)
            else:
                st.info("No data to display for the current filter selection.")

        else:
            st.warning("Data could not be loaded or is empty. Please ensure the pipeline has run successfully.")
    else:
        st.error("Failed to connect to the database. Please check the configuration and ensure the database file exists.")
    

if __name__ == '__main__':
    main()
