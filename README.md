# 📺 YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit

This project allows you to fetch, analyze, and warehouse YouTube data using the YouTube Data API v3, Streamlit for the frontend, and MySQL for storage.

---

##⚙️ Prerequisite

- pip install streamlit
- pip install google-api-python-client
- pip install mysql-connector-python
- pip install pandas

---
## 📌 Features

- Fetch YouTube channel, playlist, video, and comment metadata
- Store structured data in a MySQL database
- Query and visualize insights using Streamlit and SQL
- Modular and maintainable codebase
  
---

## 🚀 Technologies Used

- Python ≥ 3.13
- Streamlit
- MySQL
- Pandas
- Plotly & Altair
- Google API client

---

## 🧠 Project Structure
|── api_functions.py # Data extraction from YouTube API
├── sql_migration.py # Data loading into MySQL + SQL queries
├── youtube_app.py # Streamlit UI app
