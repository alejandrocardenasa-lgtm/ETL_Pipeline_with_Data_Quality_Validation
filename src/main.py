from extract import extract_data, register_in_ge, basic_profiling
from validate_input import run_input_validation
from clean import clean_data
from transform import transform_data
from validate_output import run_output_validation

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
    print("\nInput validation completed.")

    # Cleaning
    df_cleaned = clean_data(df)
    print("\nCleaning completed.")

    # Transformation
    df_transformed = transform_data(df_cleaned)
    print("\nTransformation completed.")

    # Output validation
    output_results = run_output_validation(context)
    print("\nOutput validation completed.")

    if output_results["success"]:
        print("\nPipeline completed successfully.")
    else:
        print("\nPipeline finished, but output validation failed.")

if __name__ == "__main__":
    main()