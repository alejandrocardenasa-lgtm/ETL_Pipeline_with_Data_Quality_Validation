from extract import extract_data, basic_profiling, register_in_ge

def main():

    # Task a - Extract & Profiling
    df = extract_data()
    basic_profiling(df)
    context, batch_request = register_in_ge(df)


if __name__ == "__main__":
    main()