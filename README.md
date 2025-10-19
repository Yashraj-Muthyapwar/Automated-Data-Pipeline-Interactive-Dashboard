<h1 align="center">Automated Data Pipeline & Interactive Dashboard</h1>

<p align="center"><i>
Ingest data from multiple inputs (API, web scraper, CSV), clean and unify it with Pandas, load to SQLite, and explore insights in a sleek Streamlit app. Containerized with Docker and schedulable via Airflow.
</i></p>

<p align="center"><i>⚡️ Tech Stack</i></p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/pandas-150458?logo=pandas&logoColor=white" alt="pandas">
  <img src="https://img.shields.io/badge/NumPy-013243?logo=numpy&logoColor=white" alt="NumPy">
  <img src="https://img.shields.io/badge/BeautifulSoup-4B8BBE" alt="BeautifulSoup">
  <img src="https://img.shields.io/badge/SQLite-044A64?logo=sqlite&logoColor=white" alt="SQLite">
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white" alt="Airflow">
  <img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="Docker">
</p>

---

## 📖 Overview

This project is an **end-to-end analytics pipeline**:
- **Extract** from multiple inputs (a public API, a web scraper, and a CSV).
- **Transform** with **pandas/NumPy** (type fixes, enrichment, features).
- **Load** into **SQLite** as a unified, analysis-ready table.
- **Visualize** in an interactive **Streamlit** dashboard with filters and charts.

Designed to run locally, in **Docker**, or on a schedule with **Airflow**.

---

## ✨ Features

- ✅ Modular ETL (`extract.py → transform.py → load.py`) with structured logging  
- ✅ Append-safe loads with optional de-duplication  
- ✅ Rich Streamlit UI: unified table, daily trend, and location filter  
- ✅ Single-command runner (`main.py`) + Docker image for reproducible runs  
- ✅ Optional Airflow DAG for scheduled refreshes  

---

## 🗂 Project Structure


## Quickstart

### Local
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# set environment variables (e.g., API keys) as needed
python main.py
streamlit run dashboard.py


### Docker
docker build -t data-pipeline:latest .
docker run --rm -p 8501:8501 data-pipeline:latest





