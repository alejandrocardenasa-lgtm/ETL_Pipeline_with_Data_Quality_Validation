import pandas as pd
import os

def create_star_schema(input_path='data/processed/retail_transformed.csv'):
    df = pd.read_csv(input_path)
    
    # 1. dim_date 
    dates = pd.date_range(start='2023-01-01', end='2023-12-31')
    dim_date = pd.DataFrame({'full_date': dates})
    dim_date['date_id'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.strftime('%B')
    dim_date['day_of_week'] = dim_date['full_date'].dt.day_name()
    
    # 2. dim_product
    dim_product = pd.DataFrame({'product_name': df['product'].unique()}).reset_index(drop=True)
    dim_product['product_id'] = dim_product.index + 1
    
    # 3. dim_customer
    dim_customer = pd.DataFrame({'customer_id': df['customer_id'].dropna().unique()}).reset_index(drop=True)
    dim_customer['customer_key'] = dim_customer.index + 1
    
    # 4. dim_location
    dim_location = pd.DataFrame({'country': df['country'].unique()}).reset_index(drop=True)
    dim_location['location_id'] = dim_location.index + 1
    
    # 5. fact_sales
    fact = df.copy()
    fact['date_id'] = pd.to_datetime(fact['invoice_date']).dt.strftime('%Y%m%d').astype(int)
    fact = fact.merge(dim_product, left_on='product', right_on='product_name', how='left')
    fact = fact.merge(dim_location, on='country', how='left')
    fact['sale_id'] = range(1, len(fact) + 1)
    
    fact_sales = fact[['sale_id', 'invoice_id', 'product_id', 'customer_id', 'location_id', 'date_id', 'quantity', 'price', 'total_revenue']]
    
    return dim_date, dim_product, dim_customer, dim_location, fact_sales

if __name__ == "__main__":
    dim_date, dim_product, dim_customer, dim_location, fact_sales = create_star_schema()
    print("Tablas del modelo dimensional generadas")