import pandas as pd
import great_expectations as gx
import re

# Cargamos el dataset
def extract_data():
    # Leemos el archivo CSV desde la carpeta raw
    df = pd.read_csv("Data/raw/retail_etl_dataset.csv")
    return df

# Funcion auxiliar para detectar formato de fecha
def detect_date_format(value):
    # Si el valor es nulo real de pandas
    if pd.isna(value):
        return "null_like"

    # Convertir a texto y quitar espacios
    value = str(value).strip()

    # Detectar valores tipo nulos
    if value in ["", "NULL", "N/A", "NA", "null", "n/a"]:
        return "null_like"

    # Formato YYYY-MM-DD
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return "YYYY-MM-DD"

    # Formato YYYY/MM/DD
    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", value):
        return "YYYY/MM/DD"

    # Formato DD-MM-YYYY
    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
        return "DD-MM-YYYY"

    # Cualquier otro formato
    return "other"

# Funcion para parsear fechas mixtas 
# Convierte fechas que vienen en formatos diferentes a un formato fecha real (datetime)
def parse_mixed_date(value):
    if pd.isna(value):
        return pd.NaT

    value = str(value).strip()

    if value in ["", "NULL", "N/A", "NA", "null", "n/a"]:
        return pd.NaT

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return pd.to_datetime(value, format="%Y-%m-%d", errors="coerce")

    if re.fullmatch(r"\d{4}/\d{2}/\d{2}", value):
        return pd.to_datetime(value, format="%Y/%m/%d", errors="coerce")

    if re.fullmatch(r"\d{2}-\d{2}-\d{4}", value):
        return pd.to_datetime(value, format="%d-%m-%Y", errors="coerce")

    return pd.NaT

# Profiling basico
def basic_profiling(df):
    # [x] shape
    print("\n- SHAPE -")
    print(df.shape)

    # [x] info
    print("\n- INFO -")
    df.info()

    # [x] memory
    print("\n- MEMORY USAGE -")
    print(df.memory_usage(deep=True))
    print("\nTotal memory usage:")
    print(df.memory_usage(deep=True).sum(), "bytes")

    # [x] missing count
    print("\n- MISSING COUNT -")
    print(df.isnull().sum())

    # [x] missing %
    print("\n- MISSING PERCENTAGE -")
    print(((df.isnull().sum() / len(df)) * 100).round(2))

    # [x] cardinality product
    print("\n- CARDINALITY: PRODUCT -")
    print(df["product"].nunique())

    # [x] cardinality country
    print("\n- CARDINALITY: COUNTRY -")
    print(df["country"].nunique())

    # [x] numeric stats
    print("\n- NUMERIC STATS -")
    numeric_stats = df[["quantity", "price", "total_revenue"]].agg(
        ["min", "max", "mean", "median", "std"]
    )
    print(numeric_stats)

    # [x] duplicate invoice_id
    duplicate_count = df["invoice_id"].duplicated().sum()
    print("\n- DUPLICATE invoice_id -")
    print(duplicate_count)

    # [x] total_revenue check
    calculated_total = df["quantity"] * df["price"]
    wrong_total_count = (abs(df["total_revenue"] - calculated_total) > 0.01).sum()
    print("\n- total_revenue != quantity * price (±0.01) -")
    print(wrong_total_count)

    # [x] date format distribution
    df["date_format"] = df["invoice_date"].apply(detect_date_format)
    print("\n- DATE FORMAT DISTRIBUTION -")
    print(df["date_format"].value_counts())

    # Parsear fechas para revisar futuras
    df["invoice_date_parsed"] = df["invoice_date"].apply(parse_mixed_date)

    # [x] future dates
    future_dates_count = (df["invoice_date_parsed"] > pd.Timestamp("2023-12-31")).sum()
    print("\n- FUTURE DATES (> 2023-12-31) -")
    print(future_dates_count)

    # [x] null-like dates
    null_like_count = (df["date_format"] == "null_like").sum()
    print("\n- NULL-LIKE DATE STRINGS -")
    print(null_like_count)

# Registramos en great expectations
def register_in_ge(df):
    # Creamos el contexto de Great Expectations
    context = gx.get_context()

    # Si el datasource ya existe, lo usamos
    # Si no existe, se crea
    try:
        data_source = context.get_datasource("retail_source")
    except Exception:
        data_source = context.sources.add_pandas(name="retail_source")

    # Si el asset ya existe, lo usamos
    # Si no existe, se crea
    try:
        data_asset = data_source.get_asset("retail_asset")
    except Exception:
        data_asset = data_source.add_dataframe_asset(name="retail_asset")

    # Crear batch request con el dataframe en memoria
    batch_request = data_asset.build_batch_request(dataframe=df)

    print("\nDataFrame registrado en Great Expectations.")
    return context, batch_request
