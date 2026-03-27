from extract import extract_data, register_in_ge, basic_profiling
from validate_input import run_input_validation
from clean import clean_data
from transform import transform_data
from validate_output import run_output_validation
from load import load_data
from load_dw import load_to_sqlite
from analysis import run_business_analysis

def main():
    # Extraccion
    df = extract_data()
    print("\nExtract completed.")

    # Registrar en Great Expectations
    context, batch_request = register_in_ge(df)

    # Perfilamiento de datos
    basic_profiling(df)
    print("\nProfiling completed.")

    # Validacion de entrada
    validation_results, wrong_revenue = run_input_validation(
        context,
        batch_request,
        df
    )
    print("\nInput validation completed.")

    # Limpieza de datos
    df_cleaned = clean_data(df)
    print("\nCleaning completed.")

    # Transformacion de datos
    df_transformed = transform_data(df_cleaned)
    print("\nTransformation completed.")

    # Guardar CSV limpio y transformado
    load_data(df_transformed)
    print("\nCSV Load completed.")

    # Output validation
    output_results = run_output_validation(context)
    print("\nOutput validation completed.")
    
    if output_results["success"]:
        print("\nPipeline base. Iniciando")
        
        # Modelado Dimensional y Carga a SQLite
        print("\n- INICIANDO CARGA AL DATA WAREHOUSE -")
        load_to_sqlite() 
        print("DW Load completed.")

        # KPIS
        print("\n- INICIANDO ANALISIS DE NEGOCIO Y KPIs -")
        run_business_analysis()
        print("Business Analysis completed. carpeta reports/")
        
        print("\nEl pipeline corrio completo.")
    else:
        print("\nPipeline finished, but output validation failed.")

if __name__ == "__main__":
    main()