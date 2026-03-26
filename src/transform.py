import pandas as pd
import os

def transform_data(df):
    print("\n- INICIANDO TRANSFORMACION DE DATOS -")

    # Trabajamos sobre una copia
    df_transformed = df.copy()

    # Aseguramos que invoice_date sea datetime
    df_transformed["invoice_date"] = pd.to_datetime(df_transformed["invoice_date"], errors="coerce")

    # Extraemos columnas de fecha
    df_transformed["year"] = df_transformed["invoice_date"].dt.year
    df_transformed["month"] = df_transformed["invoice_date"].dt.month
    df_transformed["day_of_week"] = df_transformed["invoice_date"].dt.day_name()

    # Crear revenue_bin con quantiles
    df_transformed["revenue_bin"] = pd.qcut(
        df_transformed["total_revenue"],
        q=3,
        labels=["Low", "Medium", "High"],
        duplicates="drop"
    )

    # Guardamos archivo transformado
    os.makedirs("data/processed", exist_ok=True)
    df_transformed.to_csv("data/processed/retail_transformed.csv", index=False)

    print("Transformaciones aplicadas con exito.")

    return df_transformed