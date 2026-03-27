import sqlite3
import pandas as pd
import os
from dimensional_model import create_star_schema

def load_to_sqlite(db_path='data/processed/data_warehouse.db'):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    dim_date, dim_product, dim_customer, dim_location, fact_sales = create_star_schema()
    
    conn = sqlite3.connect(db_path)
    
    print("Cargando dimensiones...")
    dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
    dim_product.to_sql('dim_product', conn, if_exists='replace', index=False)
    dim_customer.to_sql('dim_customer', conn, if_exists='replace', index=False)
    dim_location.to_sql('dim_location', conn, if_exists='replace', index=False)
    
    print("Cargando tabla de hechos...")
    fact_sales.to_sql('fact_sales', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"Data Warehouse guardado en {db_path}")

if __name__ == "__main__":
    load_to_sqlite()