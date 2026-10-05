# Atlas Capital — Moroccan Stock Market Data Platform

An automated, end-to-end ELT data pipeline collecting Moroccan stock market data (Casablanca Stock Exchange) from *Le Boursier*, transforming it through a Medallion Architecture, and exposing analytics-ready tables for consumption.

---

## 🏛️ Architecture Overview

The platform uses a layered Medallion Architecture:

1. **Extraction**: Selenium runs headless in a container to extract quotes from *Le Boursier*.
2. **Raw Layer**: Scraped payloads are stored as unedited date-partitioned files (`data/raw/...`) for auditability and full replayability.
3. **Bronze Layer (PostgreSQL)**: Raw data loaded idempotently via `COPY`, stored as text with audit columns (`_run_id`, `_ingested_at`, `_source_file`).
4. **Silver Layer (dbt Core)**: Cleaned, deduplicated, and typed data (handling French numeric formats).
5. **Gold Layer (dbt Core)**: Star-schema data warehouse (dimensions and facts) with analytics metrics (moving averages, rolling volumes).
6. **Orchestration**: Apache Airflow manages scheduling, dependency execution, holiday handling, and failure alerts (Telegram).

---

## 🛠️ Tech Stack & Key Choices

- **Orchestration**: Apache Airflow (LocalExecutor)
- **Extraction**: Python 3.11+, Selenium (`selenium/standalone-chrome`)
- **Storage**: PostgreSQL (Community Edition)
- **Transformation & Testing**: dbt Core (`dbt-postgres`)
- **Containerization**: Docker Compose
- **Quality & CI**: Ruff, Pytest, GitHub Actions

---

## 🚀 Quick Start (Local Setup)

### Prerequisites
- Docker & Docker Compose
- Git

### Steps
1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd atlascapitalv2
   ```
2. Set up environment configuration:
   ```bash
   cp .env.example .env
   # Edit .env with your specific credentials if needed
   ```
3. Start the platform:
   ```bash
   docker compose up -d
   ```
4. Access Airflow UI:
   - URL: `http://localhost:8080`
   - Default credentials: `admin` / `admin`

---

## ⚖️ Educational Disclaimer
This project is developed for educational and portfolio demonstration purposes.
