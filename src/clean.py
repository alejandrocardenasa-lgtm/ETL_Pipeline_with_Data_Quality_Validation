import pandas as pd
import numpy as np

def clean_data(df):
    print("\n- INICIANDO LIMPIEZA DE DATOS -")

    # Trabajamos sobre una copia
    df_clean = df.copy()

    # Normalizamos product
    df_clean["product"] = df_clean["product"].str.strip().str.title()

    # Estandarizamos country con diccionario
    country_map = {
        "ecuador": "Ecuador",
        "colombia": "Colombia",
        "CO": "Colombia"
    }

    df_clean["country"] = df_clean["country"].str.strip()
    df_clean["country"] = df_clean["country"].replace(country_map)
    df_clean["country"] = df_clean["country"].str.title()

    # Llenamos customer_id nulos con IDs nuevos
    max_id = df_clean["customer_id"].max()

    if pd.isna(max_id):
        max_id = 100000

    missing_mask = df_clean["customer_id"].isnull()
    num_missing = missing_mask.sum()

    if num_missing > 0:
        new_ids = np.arange(int(max_id) + 1, int(max_id) + 1 + num_missing)
        df_clean.loc[missing_mask, "customer_id"] = new_ids

    # Convertir a Int64
    df_clean["customer_id"] = df_clean["customer_id"].astype("Int64")

    # Arreglamos cantidades y precios
    df_clean = df_clean[df_clean["price"] >= 0.01]
    df_clean = df_clean[df_clean["quantity"] >= 1]

    # Parsear fechas
    df_clean["invoice_date"] = pd.to_datetime(
        df_clean["invoice_date"],
        format="mixed",
        errors="coerce",
        dayfirst=True
    )

    # Rellenar fechas nulas
    df_clean["invoice_date"] = df_clean["invoice_date"].ffill().bfill()

    # Corregir fechas futuras
    max_valid_date = pd.Timestamp("2023-12-31")
    df_clean.loc[df_clean["invoice_date"] > max_valid_date, "invoice_date"] = max_valid_date

    # Recalcular total_revenue
    df_clean["total_revenue"] = df_clean["quantity"] * df_clean["price"]

    # Eliminar duplicados exactos
    df_clean = df_clean.drop_duplicates()

    # Eliminar duplicados lógicos por factura + producto
    df_clean = df_clean.drop_duplicates(subset=["invoice_id", "product"], keep="first")

    print(f"Filas originales: {len(df)}")
    print(f"Filas despues de limpieza: {len(df_clean)}")

    return df_clean