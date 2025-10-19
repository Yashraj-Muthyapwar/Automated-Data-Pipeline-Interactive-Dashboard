# transform.py

import pandas as pd

"""
This script will contain all the functions responsible for
transforming the raw data extracted from the sources into a
clean and unified format, ready for loading into the database.
"""

def transform_data(weather_data, sales_df, scraped_data):
    """
    The main transformation function. It takes the raw data from all sources,
    converts them into pandas DataFrames, cleans and standardizes them,
    and finally merges them into a single DataFrame.

    Args:
        weather_data (dict): Raw data from the weather API.
        sales_df (pd.DataFrame): Raw data from the CSV file, already a DataFrame.
        scraped_data (list of dicts): Raw data from web scraping.

    Returns:
        pd.DataFrame: A single, cleaned, and unified DataFrame.
    """
    
    print("Starting data transformation process...")

    # --- 1. Transform & Convert to DataFrame ---
    # (Code from previous steps, omitted for brevity)
    if weather_data:
        transformed_weather = {
            'city': [weather_data.get('name')],
            'temperature_celsius': [weather_data.get('main', {}).get('temp')],
            'weather_condition': [weather_data.get('weather', [{}])[0].get('description')],
            'humidity_percent': [weather_data.get('main', {}).get('humidity')],
            'wind_speed_m_s': [weather_data.get('wind', {}).get('speed')],
            'report_timestamp': [pd.to_datetime(weather_data.get('dt'), unit='s')]
        }
        weather_df = pd.DataFrame(transformed_weather)
    else:
        weather_df = pd.DataFrame(columns=['city', 'temperature_celsius', 'weather_condition', 'humidity_percent', 'wind_speed_m_s', 'report_timestamp'])

    if scraped_data:
        scraped_df = pd.DataFrame(scraped_data)
    else:
        scraped_df = pd.DataFrame()

    if sales_df is None or sales_df.empty:
        sales_df = pd.DataFrame()

    # --- 2. Standardize Column Names ---
    # (Code from previous step, omitted for brevity)
    if not weather_df.empty:
        weather_df.columns = [col.lower().replace(' ', '_') for col in weather_df.columns]
    if not scraped_df.empty:
        scraped_df.columns = [col.lower().replace(' ', '_') for col in scraped_df.columns]
    if not sales_df.empty:
        sales_df.columns = [col.lower().replace(' ', '_') for col in sales_df.columns]

    # --- 3. Handle Missing Values ---
    # (Code from previous step, omitted for brevity)
    if not sales_df.empty and 'sale_amount' in sales_df.columns:
        mean_sale_amount = sales_df['sale_amount'].mean()
        sales_df['sale_amount'] = sales_df['sale_amount'].fillna(mean_sale_amount)
    
    # --- 4. Correct Data Types ---
    # (Code from previous step, omitted for brevity)
    if not scraped_df.empty:
        price_cleaned = scraped_df['price'].str.replace('£', '')
        scraped_df['book_price_gbp'] = pd.to_numeric(price_cleaned, errors='coerce') # Renaming for clarity
        scraped_df['book_price_gbp'] = scraped_df['book_price_gbp'].fillna(0.0)
        scraped_df = scraped_df.drop(columns=['price'])
        scraped_df.rename(columns={'title': 'book_title', 'availability': 'book_availability'}, inplace=True) # Rename for merge clarity
    if not sales_df.empty:
        sales_df['sale_date'] = pd.to_datetime(sales_df['sale_date'], errors='coerce')
        sales_df['product_id'] = pd.to_numeric(sales_df['product_id'], errors='coerce').fillna(0).astype(int)

    # --- 5. Feature Engineering ---
    # (Code from previous step, omitted for brevity)
    if not sales_df.empty and 'sale_date' in sales_df.columns:
        sales_df['sale_month'] = sales_df['sale_date'].dt.month
        sales_df['sale_day_name'] = sales_df['sale_date'].dt.day_name()
    
    # --- START OF NEW MERGING STEP ---
    print("\n--- Merging DataFrames ---")
    
    # Use sales_df as the base DataFrame. If it's empty, we can't proceed.
    if sales_df.empty:
        print("Sales data is empty, cannot perform merge. Aborting.")
        return pd.DataFrame()

    # Step 1: Cross-join sales data with the single-row weather data.
    # We assign the weather data to new columns in the sales_df. Since there's only one
    # row in weather_df, its values will be broadcast across all rows of sales_df.
    if not weather_df.empty:
        for col in weather_df.columns:
            sales_df[col] = weather_df[col].iloc[0]
        merged_df = sales_df
        print("Successfully merged weather data into sales data.")
    else:
        merged_df = sales_df # Proceed without weather data if it's missing

    # Step 2: Merge the scraped book data using an artificial key (the index).
    if not scraped_df.empty:
        # We'll use a left join to ensure we keep all sales records, even if there
        # are fewer books scraped than sales records.
        # We join on the DataFrame's index.
        final_df = pd.merge(
            merged_df, 
            scraped_df, 
            how='left', 
            left_index=True, 
            right_index=True
        )
        print("Successfully merged scraped book data.")
    else:
        final_df = merged_df # Proceed without book data if it's missing

    # --- START OF NEW DUPLICATE REMOVAL STEP ---
    print("\n--- Removing Duplicate Records ---")
    
    # First, let's identify and count duplicates to see the problem.
    # We define a duplicate based on the core transaction details.
    subset_cols = ['product_id', 'sale_date', 'sale_amount']
    
    # Get the number of duplicate rows before removal
    duplicates_before = final_df.duplicated(subset=subset_cols).sum()
    print(f"Found {duplicates_before} duplicate transaction records.")

    if duplicates_before > 0:
        # Use drop_duplicates to remove them, keeping the first occurrence
        final_df_cleaned = final_df.drop_duplicates(subset=subset_cols, keep='first')
        
        # Get the shape before and after to show the result
        rows_before = final_df.shape[0]
        rows_after = final_df_cleaned.shape[0]
        
        print(f"Removed {rows_before - rows_after} duplicate row(s).")
        print(f"DataFrame shape before: {final_df.shape}, after: {final_df_cleaned.shape}")
    else:
        print("No duplicate records found.")
        final_df_cleaned = final_df # No changes needed

    print("\n--- Final Cleaned DataFrame (First 5 Rows) ---")
    print(final_df_cleaned.head())
    
    print("\n--- Final Cleaned DataFrame Info ---")
    final_df_cleaned.info()
    
    return final_df_cleaned 
