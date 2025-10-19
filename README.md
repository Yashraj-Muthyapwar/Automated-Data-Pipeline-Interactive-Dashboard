# Automated Data Pipeline & Interactive Dashboard

End-to-end data project that ingests from multiple inputs (API, web scraper, CSV), cleans and transforms with **pandas/numpy**, loads into **SQLite**, and serves an interactive **Streamlit** dashboard. Orchestrated with **Airflow** and packaged in **Docker** for reproducible runs.

## Tech Stack
Python, pandas, numpy, BeautifulSoup, SQLite, Streamlit, Airflow, Docker

## Features
- Modular ETL (`extract.py` → `transform.py` → `load.py`) with structured logging
- Append-safe loads with optional de-duplication
- Interactive dashboard (table, trend, categorical charts, filters)
- Runs locally, in Docker, or on a schedule with Airflow

## Project Structure
