# ETL Pipeline with Data Quality Validation

## Project Description
This project implements an ETL pipeline with data quality validation using Great Expectations.

The pipeline follows:
Extract → Profiling → Validate Input → Clean → Transform → Load → Validate Output

## Project Structure
Data/
src/
reports/
great_expectations/

## How to Run the Project

1. Clone the repository
git clone https://github.com/alejandrocardenasa-lgtm/ETL_Pipeline_with_Data_Quality_Validation.git

2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

3. Install dependencies
pip install -r requirements.txt

4. Run ETL pipeline
python src/main.py

5. Build Great Expectations Data Docs
great_expectations docs build
