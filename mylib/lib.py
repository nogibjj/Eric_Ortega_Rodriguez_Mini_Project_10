import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

LOG_FILE = "pyspark_output.md"


def log_output(operation, output, query=None):
    """Adds to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"### Operation: {operation}\n\n")
        if query:
            file.write(f"**Query:** {query}\n\n")
        file.write("**Truncated Output:**\n\n")
        file.write(output)
        file.write("\n\n")


def start_spark(app_name="PhoneDataProcessing"):
    """Initialize and return a Spark session."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark


def end_spark(spark):
    """Stop the Spark session."""
    spark.stop()
    return "Stopped Spark session"


def extract(file_path="data/processed_data_news.csv"):
    """Return the path of the local dataset file."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{file_path} does not exist.")
    return file_path


def load_data(spark, data):
    """Load data with an inferred schema for the phone dataset."""
    schema = StructType([
        StructField("phone_brand", StringType(), True),
        StructField("phone_model", StringType(), True),
        StructField("store", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("currency", StringType(), True),
        StructField("price_USD", FloatType(), True),
        StructField("storage", IntegerType(), True),
        StructField("ram", IntegerType(), True),
        StructField("Launch", StringType(), True),
        StructField("Dimensions", StringType(), True),
        StructField("Chipset", StringType(), True),
        StructField("CPU", StringType(), True),
        StructField("GPU", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Foldable", IntegerType(), True),
        StructField("PPI_Density", IntegerType(), True),
        StructField("quantile_10", StringType(), True),
        StructField("quantile_50", StringType(), True),
        StructField("quantile_90", StringType(), True),
        StructField("price_range", StringType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv(data)
    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df


def query(spark, df, sql_query, table_name="phone_data"):
    """Run an SQL query on the DataFrame."""
    df.createOrReplaceTempView(table_name)
    result_df = spark.sql(sql_query)
    log_output("query data", result_df.limit(10).toPandas().to_markdown(), sql_query)
    return result_df.show()


def describe(df):
    """Describe the data and log the summary statistics."""
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)
    return df.describe().show()


def example_transform(df):
    """Apply a transformation to filter high-priced phones."""
    df = df.filter(df.price_range == "high price").select("phone_brand", "phone_model", "price_USD", "store")
    log_output("transform data", df.limit(10).toPandas().to_markdown())
    return df.show()
