import os
from mylib.lib import (
    load_data,
    start_spark,
    end_spark,
)
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


def generate_pdf_report(filename, content):
    """
    Generate a PDF report summarizing the outputs.
    """
    pdf = canvas.Canvas(filename, pagesize=letter)
    width, height = letter
    y = height - 50

    pdf.setFont("Helvetica", 12)
    pdf.drawString(50, y, "Data Processing Report")
    y -= 30

    for section, data in content.items():
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawString(50, y, section)
        y -= 20
        pdf.setFont("Helvetica", 10)

        for line in data.split("\n"):
            pdf.drawString(50, y, line[:95])  # Wrap long lines
            y -= 15
            if y < 50:  # Start a new page if out of space
                pdf.showPage()
                y = height - 50

    pdf.save()
    print(f"PDF report generated: {filename}")


def run_sql_query(spark, df):
    """
    Run SQL query for average price per brand.
    """
    query = (
        "SELECT phone_brand, AVG(price_USD) AS avg_price_usd "
        "FROM phone_data GROUP BY phone_brand "
        "ORDER BY avg_price_usd DESC"
    )
    df.createOrReplaceTempView("phone_data")
    return (
        spark.sql(query)
        .toPandas()
        .to_string()
    )


def main():
    """
    Main entry point for the PySpark CLI application.
    """
    try:
        # Initialize content dictionary for PDF
        content = {}

        # Step 1: Extract data path
        print("Extracting data path...")
        base_path = "ericortega/Eric_Ortega_Rodriguez_Mini_Project_10"
        data_filename = "data/processed_data_news.csv"
        data_path = os.getenv(
            "DATASET_PATH", 
            os.path.join(base_path, data_filename)
        )
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Dataset not found at: {data_path}")
        content["Extracting Data Path"] = f"Data Path: {data_path}"

        # Step 2: Start Spark session
        spark = start_spark("PhoneDataProcessing")
        print("Spark session started successfully.")
        content["Starting Spark Session"] = "Spark session started successfully."

        # Step 3: Load data
        print(f"Loading data from {data_path}...")
        df = load_data(spark, data=data_path)
        if df is None:
            raise ValueError("Error: Failed to load data.")
        row_count = df.count()
        content["Loading Data"] = f"Data loaded successfully with {row_count} rows."

        # Step 4: Run SQL query
        print("Running SQL query for average price per brand...")
        query_results = run_sql_query(spark, df)
        content["SQL Query"] = query_results
        print("SQL query executed successfully.")

    except Exception as e:
        content["Error"] = f"An error occurred: {e}"
        print(f"An error occurred: {e}")

    finally:
        # Step 5: End Spark session
        if "spark" in locals():
            end_spark(spark)
            print("Spark session ended. Processing completed successfully.")
            content["Ending Spark Session"] = "Spark session ended successfully."

        # Generate PDF report
        generate_pdf_report("output_report.pdf", content)


if __name__ == "__main__":
    main()