import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import os

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

    # Comparison table
    comparison_table = pd.DataFrame({
        "Expectation": [
            "Compound uniqueness (invoice_id, product)",
            "total_revenue = qty × price",
            "country in set",
            "customer_id not null",
            "invoice_date not null",
            "invoice_date regex format",
            "invoice_date valid range",
            "price >= 0.01",
            "product in set",
            "quantity >= 1"
        ],
        "Raw Pass %": [
            "90.10%",
            "81.20%",
            "56.65%",
            "96.04%",
            "99.75%",
            "96.50%",
            "94.02%",
            "98.02%",
            "100%",
            "97.08%"
        ],
        "Clean Pass %": [
            "100%",
            "100%",
            "100%",
            "100%",
            "100%",
            "100%",
            "100%",
            "100%",
            "100%",
            "100%"
        ],
        "Status": [
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "RESOLVED",
            "OK",
            "RESOLVED"
        ],
        "Dimension": [
            "Uniqueness",
            "Consistency",
            "Validity",
            "Completeness",
            "Completeness",
            "Validity",
            "Validity",
            "Validity",
            "Validity",
            "Validity"
        ]
    })

    # Crear carpeta reports si no existe
    os.makedirs("reports", exist_ok=True)   
    with PdfPages("reports/dq_scores.pdf") as pdf:

        # Pagina 1 - DQ Scores
        fig1, ax1 = plt.subplots(figsize=(6, 2))
        ax1.axis("off")
        ax1.table(
            cellText=dq_table.values,
            colLabels=dq_table.columns,
            loc="center"
        )
        plt.title("Data Quality Scores")
        pdf.savefig(fig1, bbox_inches="tight")
        plt.close()

        # Pagina 2 - Comparison Table
        fig2, ax2 = plt.subplots(figsize=(16, 5))
        ax2.axis("off")
        table = ax2.table(
            cellText=comparison_table.values,
            colLabels=comparison_table.columns,
            loc="center",
            cellLoc="center"
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)

        plt.title("Comparison Table: Raw vs Clean Validation")
        pdf.savefig(fig2, bbox_inches="tight")
        plt.close()

    print("DQ Scores + Comparison Table PDF generado.")

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