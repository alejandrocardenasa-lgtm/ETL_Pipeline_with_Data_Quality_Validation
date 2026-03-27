import pandas as pd
import great_expectations as gx

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

    # Intentar formatos conocidos
    for fmt, name in [
        ("%Y-%m-%d", "YYYY-MM-DD"),
        ("%Y/%m/%d", "YYYY/MM/DD"),
        ("%d-%m-%Y", "DD-MM-YYYY"),
    ]:
        try:
            pd.to_datetime(value, format=fmt)
            return name
        except:
            continue

    # Cualquier otro formato
    return "other"

# Fechas futuras
def count_future_dates(date_series):
    parsed_dates = pd.to_datetime(date_series, errors="coerce")
    future_dates_count = (parsed_dates > pd.Timestamp("2023-12-31")).sum()
    return future_dates_count

# Profiling basico
def basic_profiling(df):
    # shape
    print("\n- SHAPE -")
    print(df.shape)

    # info
    print("\n- INFO -")
    df.info()

    # memoria
    print("\n- MEMORY USAGE -")
    print(df.memory_usage(deep=True))
    print("\nTotal memory usage:")
    print(df.memory_usage(deep=True).sum(), "bytes")

    # conteo de nulos
    print("\n- MISSING COUNT -")
    print(df.isnull().sum())

    # porcentaje de nulos
    print("\n- MISSING PERCENTAGE -")
    print(((df.isnull().sum() / len(df)) * 100).round(2))

    # cardinalidad product
    print("\n- CARDINALITY: PRODUCT -")
    print(df["product"].nunique())

    # cardinalidad country
    print("\n- CARDINALITY: COUNTRY -")
    print(df["country"].nunique())

    # conteo de valores product
    print("\n- PRODUCT VALUE COUNTS -")
    print(df["product"].value_counts())

    # conteo de valores country
    print("\n- COUNTRY VALUE COUNTS -")
    print(df["country"].value_counts())

    # valores unicos product
    print("\n- PRODUCT UNIQUE VALUES -")
    print(df["product"].unique())

    # valores unicos country
    print("\n- COUNTRY UNIQUE VALUES -")
    print(df["country"].unique())

    # estadisticas numericas
    print("\n- NUMERIC STATS -")
    numeric_stats = df[["quantity", "price", "total_revenue"]].agg(
        ["min", "max", "mean", "median", "std"]
    )
    print(numeric_stats)

    # invoice_id duplicados
    duplicate_count = df["invoice_id"].duplicated().sum()
    print("\n- DUPLICATE invoice_id -")
    print(duplicate_count)

    # verificacion total_revenue
    calculated_total = df["quantity"] * df["price"]
    wrong_total_count = (abs(df["total_revenue"] - calculated_total) > 0.01).sum()
    print("\n- total_revenue != quantity * price (±0.01) -")
    print(wrong_total_count)

    # distribucion de formatos de fecha
    df["date_format"] = df["invoice_date"].apply(detect_date_format)
    print("\n- DATE FORMAT DISTRIBUTION -")
    print(df["date_format"].value_counts())

    # fechas tipo null
    null_like_count = (df["date_format"] == "null_like").sum()
    print("\n- NULL-LIKE DATE STRINGS -")
    print(null_like_count)

    # formatos de fecha invalidos
    other_format_count = (df["date_format"] == "other").sum()
    print("\n- OTHER / INVALID DATE FORMATS -")
    print(other_format_count)

    # fechas futuras
    future_dates_count = count_future_dates(df["invoice_date"])
    print("\n- FUTURE DATES (> 2023-12-31) -")
    print(future_dates_count)

    # muestra de fechas invalidas
    print("\n- SAMPLE INVALID DATE VALUES -")
    print(df[df["date_format"] == "other"]["invoice_date"].head())

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