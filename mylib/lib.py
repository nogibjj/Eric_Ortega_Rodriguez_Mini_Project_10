import os
from pyspark.sql import SparkSession


def extract():
    """
    Returns the path to the dataset file.
    Validates the existence of the dataset file.
    """
    path = (
        "/workspaces/Eric_Ortega_Rodriguez_Mini_Project_10/data/"
        "processed_data_news.csv"
    )
    if not os.path.exists(path):
        raise FileNotFoundError(f"Dataset not found at: {path}")
    return path


def start_spark(app_name):
    """
    Starts and returns a Spark session.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark
    except Exception as e:
        raise RuntimeError(f"Failed to start Spark session: {e}")


def load_data(spark, data):
    """
    Loads a CSV file into a Spark DataFrame.
    """
    try:
        df = spark.read.csv(data, header=True, inferSchema=True)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to load data: {e}")


def describe(df):
    """
    Prints the schema and a sample of the dataset.
    """
    try:
        print("Dataset Schema:")
        df.printSchema()
        print("Sample Data:")
        df.show(5)
    except Exception as e:
        raise RuntimeError(f"Failed to describe DataFrame: {e}")


def query(spark, df, sql_query, table_name):
    """
    Executes a SQL query on the provided DataFrame.
    """
    try:
        df.createOrReplaceTempView(table_name)
        result = spark.sql(sql_query)
        print("Query Result:")
        result.show()
    except Exception as e:
        raise RuntimeError(f"Failed to execute query: {e}")


def example_transform(df):
    """
    Prints the transformed DataFrame.
    """
    try:
        print("Transformed DataFrame:")
        df.show(5)
    except Exception as e:
        raise RuntimeError(f"Failed to apply transformation: {e}")


def end_spark(spark):
    """
    Stops the Spark session.
    """
    try:
        spark.stop()
    except Exception as e:
        print(f"Failed to stop Spark session: {e}")

# import os
# from pyspark.sql import SparkSession


# def extract():
#     """
#     Returns the path to the dataset file.
#     Validates the existence of the dataset file.
#     """
#     # Path to the dataset
# def extract():
#     path = (
#         "/workspaces/Eric_Ortega_Rodriguez_Mini_Project_10/data/"
#         "processed_data_news.csv"
#     )
#     if not os.path.exists(path):
#         raise FileNotFoundError(f"Dataset not found at: {path}")
#     return path


# def start_spark(app_name):
#     """
#     Starts and returns a Spark session.
#     """
#     try:
#         spark = SparkSession.builder.appName(app_name).getOrCreate()
#         return spark
#     except Exception as e:
#         raise RuntimeError(f"Failed to start Spark session: {e}")


# def load_data(spark, data):
#     """
#     Loads a CSV file into a Spark DataFrame.
#     """
#     try:
#         df = spark.read.csv(data, header=True, inferSchema=True)
#         return df
#     except Exception as e:
#         raise RuntimeError(f"Failed to load data: {e}")


# def describe(df):
#     """
#     Prints the schema and a sample of the dataset.
#     """
#     try:
#         print("Dataset Schema:")
#         df.printSchema()
#         print("Sample Data:")
#         df.show(5)
#     except Exception as e:
#         raise RuntimeError(f"Failed to describe DataFrame: {e}")


# def query(spark, df, sql_query, table_name):
#     """
#     Executes a SQL query on the provided DataFrame.
#     """
#     try:
#         df.createOrReplaceTempView(table_name)
#         result = spark.sql(sql_query)
#         print("Query Result:")
#         result.show()
#     except Exception as e:
#         raise RuntimeError(f"Failed to execute query: {e}")


# def example_transform(df):
#     """
#     Prints the transformed DataFrame.
#     """
#     try:
#         print("Transformed DataFrame:")
#         df.show(5)
#     except Exception as e:
#         raise RuntimeError(f"Failed to apply transformation: {e}")


# def end_spark(spark):
#     """
#     Stops the Spark session.
#     """
#     try:
#         spark.stop()
#     except Exception as e:
#         print(f"Failed to stop Spark session: {e}")
