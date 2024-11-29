from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from mylib.lib import (
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def generate_pdf(output_file, steps_output):
    """
    Generate a PDF report summarizing the outputs of each step.
    """
    c = canvas.Canvas(output_file, pagesize=letter)
    c.setFont("Helvetica", 10)

    # Add a title
    c.drawString(30, 750, "PySpark Data Processing Report")
    c.drawString(30, 735, "-" * 50)

    y_position = 715
    for step, output in steps_output.items():
        if y_position < 50:  # Create a new page if content overflows
            c.showPage()
            y_position = 750
        c.drawString(30, y_position, f"Step: {step}")
        y_position -= 15
        for line in output.split("\n"):
            c.drawString(30, y_position, line[:95])  # Wrap long lines
            y_position -= 12

    c.save()
    print(f"PDF report generated: {output_file}")


def main():
    """
    Main entry point for the PySpark CLI application.
    """
    steps_output = {}  # To store the output of each step for the report
    output_file = "data_processing_report.pdf"

    try:
        # Step 1: Hardcoded data path
        print("Extracting data path...")
        data_path = (
            "ericortega/Eric_Ortega_Rodriguez_Mini_Project_10/data/"
            "processed_data_news.csv"
        )
        steps_output["Extracting Data Path"] = f"Data Path: {data_path}"

        # Step 2: Start Spark session
        spark = start_spark("PhoneDataProcessing")
        print("Spark session started successfully.")
        steps_output["Starting Spark Session"] = "Spark session started successfully."

        # Step 3: Load data into a Spark DataFrame
        print(f"Loading data from {data_path}...")
        df = load_data(spark, data=data_path)
        if df is None:
            steps_output["Loading Data"] = "Error: Failed to load data."
            print("Error: Failed to load data.")
            end_spark(spark)
            return
        steps_output["Loading Data"] = "Data loaded successfully into Spark DataFrame."

        # Step 4: Describe the data (generate summary statistics or schema)
        print("Describing the dataset...")
        describe_output = []
        df.printSchema(lambda line: describe_output.append(line))
        steps_output["Describing Dataset"] = "\n".join(describe_output)

        # Step 5: SQL Query - Calculate average price in USD per phone brand
        print("Running SQL query for average price per brand...")
        query_output = query(
            spark,
            df,
            """
            SELECT phone_brand, AVG(price_USD) AS avg_price_usd
            FROM phone_data
            GROUP BY phone_brand
            ORDER BY avg_price_usd DESC
            """,
            table_name="phone_data",
        )
        steps_output["SQL Query"] = query_output

        # Step 6: Example Transformation - Filter and select specific columns
        print("Applying transformation: Filtering high-priced phones...")
        high_price_phones = (
            df.filter(df.price_range == "high price")
            .select("phone_brand", "phone_model", "price_USD", "store")
        )
        transformation_output = []
        high_price_phones.show(5, truncate=False, output=transformation_output.append)
        steps_output["Data Transformation"] = "\n".join(transformation_output)

    except Exception as e:
        steps_output["Error"] = f"An error occurred: {e}"
        print(f"An error occurred: {e}")

    finally:
        # Step 7: End Spark session
        if 'spark' in locals():
            end_spark(spark)
            print("Spark session ended. Processing completed successfully.")
            steps_output["Ending Spark Session"] = "Spark session ended successfully."

        # Generate the PDF report
        generate_pdf(output_file, steps_output)


if __name__ == "__main__":
    main()



# from mylib.lib import (
#     load_data,
#     describe,
#     query,
#     example_transform,
#     start_spark,
#     end_spark,
# )


# def main():
#     """
#     Main entry point for the PySpark CLI application.
#     """
#     try:
#         # Step 1: Hardcoded data path
#         print("Extracting data path...")
#         data_path = (
#             "ericortega/Eric_Ortega_Rodriguez_Mini_Project_10/data/"
#             "processed_data_news.csv"
#         )

#         # Step 2: Start Spark session
#         spark = start_spark("PhoneDataProcessing")
#         print("Spark session started successfully.")

#         # Step 3: Load data into a Spark DataFrame
#         print(f"Loading data from {data_path}...")
#         df = load_data(spark, data=data_path)
#         if df is None:
#             print("Error: Failed to load data.")
#             end_spark(spark)
#             return
#         print("Data loaded successfully into Spark DataFrame.")

#         # Step 4: Describe the data (generate summary statistics or schema)
#         print("Describing the dataset...")
#         describe(df)

#         # Step 5: SQL Query - Calculate average price in USD per phone brand
#         print("Running SQL query for average price per brand...")
#         query(
#             spark,
#             df,
#             """
#             SELECT phone_brand, AVG(price_USD) AS avg_price_usd
#             FROM phone_data
#             GROUP BY phone_brand
#             ORDER BY avg_price_usd DESC
#             """,
#             table_name="phone_data",
#         )

#         # Step 6: Example Transformation - Filter and select specific columns
#         print("Applying transformation: Filtering high-priced phones...")
#         high_price_phones = (
#             df.filter(df.price_range == "high price")
#             .select("phone_brand", "phone_model", "price_USD", "store")
#         )
#         example_transform(high_price_phones)

#     except Exception as e:
#         print(f"An error occurred: {e}")
    
#     finally:
#         # Step 7: End Spark session
#         if 'spark' in locals():
#             end_spark(spark)
#             print("Spark session ended. Processing completed successfully.")


# if __name__ == "__main__":
#     main()

####################################OLD CODE#############
# from mylib.lib import (
#     extract,
#     load_data,
#     describe,
#     query,
#     example_transform,
#     start_spark,
#     end_spark,
# )



# def main():
#     """
#     Main entry point for the PySpark CLI application.
#     """
#     # Step 1: Extract data path (assumes your extract function fetches a valid path)
#     data_path = extract()
#     if not data_path:
#         print("Error: Data path not found.")
#         return

#     # Step 2: Start Spark session
#     spark = start_spark("PhoneDataProcessing")
#     print("Spark session started successfully.")

#     # Step 3: Load data into a Spark DataFrame
#     df = load_data(spark, data=data_path)
#     if df is None:
#         print("Error: Failed to load data.")
#         end_spark(spark)
#         return
#     print("Data loaded successfully into Spark DataFrame.")

#     # Step 4: Describe the data (generate summary statistics or schema)
#     print("Describing the dataset...")
#     describe(df)

#     # Step 5: SQL Query - Calculate average price in USD per phone brand
#     print("Running SQL query for average price per brand...")
#     query(
#         spark,
#         df,
#         """
#         SELECT phone_brand, AVG(price_USD) AS avg_price_usd
#         FROM phone_data
#         GROUP BY phone_brand
#         ORDER BY avg_price_usd DESC
#         """,
#         table_name="phone_data",
#     )

#     # Step 6: Example Transformation - Filter and select specific columns
#     print("Applying transformation: Filtering high-priced phones...")
#     high_price_phones = (
#         df.filter(df.price_range == "high price")
#         .select("phone_brand", "phone_model", "price_USD", "store")
#     )
#     example_transform(high_price_phones)

#     # Step 7: End Spark session
#     end_spark(spark)
#     print("Spark session ended. Processing completed successfully.")


# if __name__ == "__main__":
#     main()
