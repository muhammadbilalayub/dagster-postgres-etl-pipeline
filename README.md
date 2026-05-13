# 🚀 Dagster PostgreSQL ETL Pipeline

A modern **ETL pipeline built using Dagster, Python, Pandas, PostgreSQL, and Docker**.

This project demonstrates how to build a **production-style data pipeline** that:
- Extracts data from CSV files
- Transforms data using Python (Pandas)
- Loads data into PostgreSQL database
- Orchestrates everything using Dagster assets
- Runs locally or inside Docker containers

---

## 📌 Tech Stack

- 🐍 Python 3.11
- 🧱 Dagster (Data Orchestration)
- 🐘 PostgreSQL (Database)
- 🐼 Pandas (Data Transformation)
- 🐳 Docker & Docker Compose

---

## 📂 Project Structure
dagster_postgres_etl/
│
├── dagster_postgres_etl/
│ ├── assets.py # ETL logic (extract, transform, load)
│ ├── resources.py # DB connection & resources
│ ├── definitions.py # Dagster pipeline definitions
│
├── data/
│ ├── customers.csv
│ ├── payments.csv
│ ├── orders.csv
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md


---

## ⚙️ Features

### ✅ ETL Pipeline
- Extract data from CSV files
- Clean & transform data using Pandas
- Load into PostgreSQL tables

### ✅ Dagster Orchestration
- Asset-based pipeline design
- Dependency management between assets
- Materialization support

### ✅ Docker Support
- Fully containerized pipeline
- One command setup using Docker Compose

---

## 🚀 Setup Instructions (Local)

### 1️⃣ Clone Repository
```bash
git clone https://github.com/muhammadbilalayub/dagster-postgres-etl-pipeline.git
cd dagster-postgres-etl-pipeline

2️⃣ Create Virtual Environment
python -m venv venv
venv\Scripts\activate   # Windows
3️⃣ Install Dependencies
pip install -r requirements.txt
4️⃣ Run Dagster UI
dagster dev

Open:

http://localhost:3000

🐳 Run with Docker (Recommended)
1️⃣ Build & Run Containers
docker compose up --build
2️⃣ Access Dagster UI
http://localhost:3000
🗄️ Database Configuration

Create a .env file in root directory:
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=dagster_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
⚠️ Common Issues & Fixes
❌ FileNotFoundError (CSV)

✔ Solution:

Ensure data/ folder exists
In Docker, ensure volume is mounted properly
❌ KeyError: column not found

✔ Solution:

Check CSV column names
Example: customer_id must exist exactly in file

❌ Docker COPY error
✔ Solution:


Ensure data/ folder is in root directory


Build from correct folder



❌ Docker not found / CLI error
✔ Solution:


Install Docker Desktop


Restart system after installation


Use "Sign in with browser"



📊 Pipeline Flow
CSV Files   ↓Dagster Assets   ↓Pandas Transformation   ↓PostgreSQL Tables   ↓Dagster UI (Materialization View)

👨‍💻 Author
Muhammad Bilal Ayub
GitHub: https://github.com/muhammadbilalayub

📌 Learning Outcome
This project helps you understand:


ETL pipelines


Dagster asset orchestration


Docker containerization


PostgreSQL integration


Real-world data engineering workflow







