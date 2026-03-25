from extract import extract_data, register_in_ge, basic_profiling
from validate_input import run_input_validation
from clean import clean_data
from transform import transform_data
from load import load_data

def main():
    # Extract
    df = extract_data()
    print("\nExtract completed.")

    # Register in Great Expectations
    context, batch_request = register_in_ge(df)

    # Profiling
    basic_profiling(df)
    print("\nProfiling completed.")

    # Input validation
    validation_results, wrong_revenue = run_input_validation(
        context,
        batch_request,
        df
    )

    # cleaning
    df_cleaned = clean_data(df)

    #Transformation
    df_transformed = transform_data(df_cleaned)

    load_data(df_transformed)

if __name__ == "__main__":
    main()