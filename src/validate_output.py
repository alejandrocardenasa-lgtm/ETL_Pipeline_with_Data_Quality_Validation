import pandas as pd
import matplotlib.pyplot as plt

def run_output_validation(context):
    suite_name = "raw_data_validation_suite"

    # Leer archivo transformado
    df = pd.read_csv("data/processed/retail_transformed.csv")

    # Convertir invoice_date a datetime
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    # Crear batch request con el csv transformado
    datasource = context.sources.add_or_update_pandas(name="output_datasource")
    try:
        asset = datasource.add_dataframe_asset(name="output_asset")
    except:
        asset = datasource.get_asset("output_asset")

    batch_request = asset.build_batch_request(dataframe=df)

    # Reusar la misma suite
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    # Agregar expectations nuevas para output
    validator.expect_column_values_to_not_be_null("invoice_date")

    validator.expect_column_values_to_not_be_null("month")
    validator.expect_column_values_to_be_between(
        "month",
        min_value=1,
        max_value=12
    )

    validator.expect_column_values_to_be_in_set(
        "country",
        ["Colombia", "Ecuador", "Peru", "Chile"]
    )

    validator.expect_column_values_to_be_in_set(
        "revenue_bin",
        ["Low", "Medium", "High"]
    )

    validator.expect_column_values_to_be_between(
        "total_revenue",
        min_value=0.000001
    )

    validator.expect_column_values_to_not_be_null("invoice_id")
    validator.expect_compound_columns_to_be_unique(
        ["invoice_id", "product"]
    )

    # Guardar suite actualizada
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Validar
    validation_results = validator.validate()

    # Leer DQ raw
    try:
        with open("dq_raw.txt", "r") as f:
            dq_raw = float(f.read())
    except:
        dq_raw = 0

    # DQ clean
    total_expectations = validation_results["statistics"]["evaluated_expectations"]
    successful_expectations = validation_results["statistics"]["successful_expectations"]
    dq_clean = successful_expectations / total_expectations

    # Tabla
    dq_table = pd.DataFrame({
        "Dataset": ["Raw Data", "Clean / Transformed Data"],
        "DQ Score (%)": [round(dq_raw * 100, 2), round(dq_clean * 100, 2)]
    })

    # PDF
    fig, ax = plt.subplots(figsize=(6, 2))
    ax.axis("off")
    ax.table(
        cellText=dq_table.values,
        colLabels=dq_table.columns,
        loc="center"
    )

    plt.savefig("dq_scores.pdf", bbox_inches="tight")
    plt.close()

    print("DQ Scores PDF generado.")

    # Checkpoint para Data Docs
    context.add_or_update_checkpoint(
        name="output_data_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name
            }
        ]
    )

    context.run_checkpoint(checkpoint_name="output_data_checkpoint")
    context.build_data_docs()

    print("\n- OUTPUT DATA VALIDATION RESULTS -")
    print("Validation success:", validation_results["success"])

    return validation_results