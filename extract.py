# extract.py

import requests
import os
from dotenv import load_dotenv
import pandas as pd
from bs4 import BeautifulSoup # Import the BeautifulSoup library
from pprint import pprint

load_dotenv()

# ... (The get_weather_data and read_csv_data functions remain unchanged) ...

def get_weather_data(api_key, city):
    """
    Fetches current weather data for a given city from the OpenWeatherMap API.
    Includes robust error handling for network and API-specific issues.

    Args:
        api_key (str): The API key for authenticating with OpenWeatherMap.
        city (str): The name of the city for which to fetch weather data.

    Returns:
        dict or None: A dictionary containing the JSON response from the API on success,
                      or None if an error occurs.
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Status Code: {response.status_code}, Response Text: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")
    
    return None

def read_csv_data(file_path):
    """
    Reads data from a local CSV file into a pandas DataFrame.
    Includes error handling for file-not-found and parsing errors.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        pd.DataFrame or None: A pandas DataFrame containing the CSV data on success,
                              or None if an error occurs.
    """
    try:
        df = pd.read_csv(file_path)
        print(f"Successfully loaded data from {file_path}")
        return df
    except FileNotFoundError:
        print(f"Error: The file was not found at {file_path}")
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {file_path} is empty.")
    except Exception as err:
        print(f"An unexpected error occurred while reading the CSV: {err}")
    
    return None

def get_page_html(url):
    """
    Fetches the raw HTML content of a webpage.

    Args:
        url (str): The URL of the webpage to fetch.

    Returns:
        str or None: The HTML content of the page as a string on success,
                     or None if an error occurs.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred while fetching {url}: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred during the request to {url}: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred while fetching {url}: {err}")

    return None

def scrape_book_listings(url):
    """
    Scrapes the main page of books.toscrape.com, extracts book details,
    and returns them as a list of dictionaries.

    Args:
        url (str): The URL of the webpage to scrape.

    Returns:
        list of dicts or None: A list where each dictionary represents a book's details.
                               Returns None if the page cannot be processed.
    """
    html_content = get_page_html(url)
    
    if not html_content:
        print("Could not fetch HTML content, aborting scraping.")
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    book_containers = soup.find_all('article', class_='product_pod')
    
    if not book_containers:
        print("No book containers found on the page.")
        return [] # Return an empty list if no books are found

    # This list will hold all the dictionaries of book data we extract.
    all_books_data = []

    # Loop through each book container we found.
    for container in book_containers:
        try:
            # --- Extract Title ---
            # We find the <h3> tag, then its child <a> tag, and get the 'title' attribute.
            title = container.h3.a['title']

            # --- Extract Price ---
            # We find the <p> tag with class 'price_color' and get its text content.
            price_str = container.find('p', class_='price_color').text

            # --- Extract Stock Availability ---
            # We find the <p> tag with class 'instock availability', get its text,
            # and use .strip() to remove extra whitespace.
            availability = container.find('p', class_='instock availability').text.strip()

            # Structure the extracted data into a dictionary.
            book_data = {
                'title': title,
                'price': price_str,
                'availability': availability
            }
            
            # Add the dictionary for this book to our main list.
            all_books_data.append(book_data)
        
        except (AttributeError, TypeError) as e:
            # This is a robust way to handle cases where a book's HTML might be
            # malformed or missing one of the elements we're looking for.
            # We print a warning and continue to the next book.
            print(f"Warning: Skipping a book due to missing data. Error: {e}")
            continue # Go to the next iteration of the loop
            
    print(f"Successfully extracted data for {len(all_books_data)} books.")
    return all_books_data

if __name__ == '__main__':
    # ... (Tests for API and CSV extraction remain the same) ...
    # --- Test API Extraction ---
    print("--- Testing API Data Extraction ---")
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    if not API_KEY:
        raise ValueError("API key not found. Please set the OPENWEATHER_API_KEY environment variable.")
    CITY_NAME = "London"
    weather_data = get_weather_data(API_KEY, CITY_NAME)
    if weather_data:
        print("Successfully fetched weather data!")
    else:
        print("Failed to fetch weather data.")

    print("\n" + "="*50 + "\n")

    # --- Test CSV Extraction ---
    print("--- Testing CSV Data Extraction ---")
    CSV_FILE_PATH = "data/sales_data.csv"
    sales_df = read_csv_data(CSV_FILE_PATH)
    if sales_df is not None:
        print("CSV data loaded successfully.")
    else:
        print("Failed to load data from CSV.")

    print("\n" + "="*50 + "\n")

    # --- MODIFIED TEST FOR FINAL SCRAPED DATA ---
    print("--- Testing Web Scraper for Final Data Extraction ---")
    SCRAPE_URL = "http://books.toscrape.com/"
    
    # Call our final scraping function.
    extracted_books = scrape_book_listings(SCRAPE_URL)
    
    # Check if we got a list of books back.
    if extracted_books:
        print(f"Successfully scraped {len(extracted_books)} books.")
        
        # To verify, let's print the details of the first two books.
        # We use pprint for a more readable dictionary output.
        print("\nHere's the data for the first two books:")
        pprint(extracted_books[:2])
    else:
        print(f"Failed to scrape book listings from {SCRAPE_URL}")
    # --- END OF MODIFIED TEST ---
