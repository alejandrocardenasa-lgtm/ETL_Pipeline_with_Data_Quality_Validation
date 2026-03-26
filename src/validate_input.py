import pandas as pd
import os
import matplotlib.pyplot as plt

  # failure rate summary table
def build_failure_summary(results, wrong_revenue, df):
    rows = []

    for result in results["results"]:
        expectation = result["expectation_config"]["expectation_type"]
        column = result["expectation_config"]["kwargs"].get("column", "N/A")
        success = result["success"]

        result_info = result.get("result", {})
        total = result_info.get("element_count", len(df))
        failed = result_info.get("unexpected_count", 0)

        if total > 0:
            failure_rate = round((failed / total) * 100, 2)
        else:
            failure_rate = 0

        rows.append([
            expectation,
            column,
            success,
            failed,
            total,
            failure_rate
        ])

    # Custom rule total_revenue
    wrong_count = len(wrong_revenue)
    total_rows = len(df)
    failure_rate_revenue = round((wrong_count / total_rows) * 100, 2)

    rows.append([
        "custom_total_revenue",
        "total_revenue",
        wrong_count == 0,
        wrong_count,
        total_rows,
        failure_rate_revenue
    ])

    summary_df = pd.DataFrame(rows, columns=[
        "expectation",
        "column",
        "success",
        "failed_rows",
        "total_rows",
        "failure_rate_%"
    ])

    return summary_df

def run_input_validation(context, batch_request, df):
    # Nombre de la expectation suite
    suite_name = "raw_data_validation_suite"

    # Columna auxiliar para accuracy
    df["expected_revenue"] = df["quantity"] * df["price"]
    
    # Creamos la suite si no existe
    try:
        context.get_expectation_suite(suite_name)
    except Exception:
        context.add_expectation_suite(expectation_suite_name=suite_name)

    # Creamos el validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )

    # Limpiamos expectativas viejas para no duplicarlas
    validator.expectation_suite.expectations = []

    # Completeness
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_not_be_null("invoice_date")

    # Uniqueness
    validator.expect_compound_columns_to_be_unique(
    column_list=["invoice_id", "product"]
    )

    # Validity
    validator.expect_column_values_to_be_between(
        "quantity",
        min_value=1
    )

    validator.expect_column_values_to_be_between(
        "price",
        min_value=0.01
    )

    validator.expect_column_values_to_be_in_set(
        "product",
        [
            "Mouse",
            "Printer",
            "Monitor",
            "Phone",
            "Laptop",
            "Headphones",
            "Keyboard",
            "Tablet"
        ]
    )

    # Consistency
    validator.expect_column_values_to_be_in_set(
        "country",
        [
            "Colombia",
            "Ecuador",
            "Chile",
            "Peru"
        ]
    )

    # Timeliness
    validator.expect_column_values_to_match_regex(
        "invoice_date",
        r"^\d{4}-\d{2}-\d{2}$"
    )

    validator.expect_column_values_to_be_between(
        "invoice_date",
        min_value="2023-01-01",
        max_value="2023-12-31"
    )

    # Accuracy
    validator.expect_column_pair_values_to_be_equal(
        "total_revenue",
        "expected_revenue"
    )

    # Guardamos suite
    validator.save_expectation_suite(discard_failed_expectations=False)

    # Validacion
    validation_results = validator.validate()

    # Crear o actualizar checkpoint
    context.add_or_update_checkpoint(
    name="raw_data_checkpoint",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": suite_name
        }
    ]
)
    
    # Ejecutar checkpoint para que aparezca en Data Docs
    context.run_checkpoint(checkpoint_name="raw_data_checkpoint")

    # Accuracy check custom
    # total_revenue == quantity * price

    difference = df["total_revenue"] - df["expected_revenue"]
    absolute_difference = difference.abs()
    wrong_revenue = df[absolute_difference > 0.01]

    print("\n- total_revenue != quantity * price (±0.01) -")
    print(len(wrong_revenue))

    print("\n- INPUT DATA VALIDATION RESULTS -")
    print("Validation success:", validation_results["success"])

    # Construir Data Docs
    context.build_data_docs()

    print("\n- Data Docs generated -.")
    print("Input validation completed.")

   # Failure rate summary PDF
    summary_df = build_failure_summary(validation_results, wrong_revenue, df)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.axis("off")
    ax.table(
        cellText=summary_df.values,
        colLabels=summary_df.columns,
        loc="center"
    )

    plt.savefig("reports/failure_rate_summary.pdf", bbox_inches="tight")
    plt.close()
    
    # DQ Score RAW
    total_expectations = validation_results["statistics"]["evaluated_expectations"]
    successful_expectations = validation_results["statistics"]["successful_expectations"]

    dq_raw = successful_expectations / total_expectations

    with open("dq_raw.txt", "w") as f:
        f.write(str(dq_raw))

    return validation_results, wrong_revenue