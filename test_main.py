import os
from pyspark.sql import SparkSession
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    start_spark,
    end_spark,
)


def test_extract():
    """
    Test if the extract function returns a valid path.
    """
    path = extract()
    assert isinstance(path, str), "Extracted path is not a string."
    assert os.path.exists(path), f"Path does not exist: {path}"


def test_load_data():
    """
    Test if data loads correctly into a DataFrame.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = extract()
    df = load_data(spark, data=path)
    assert df is not None, "DataFrame is None."
    assert df.count() > 0, "DataFrame is empty."
    spark.stop()


def test_query():
    """
    Test the query function with a sample SQL query.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = extract()
    df = load_data(spark, data=path)

    # Run a sample query
    query(
        spark,
        df,
        """
        SELECT phone_brand, COUNT(*) AS count
        FROM phone_data
        GROUP BY phone_brand
        """,
        "phone_data",
    )
    spark.stop()


def test_describe():
    """
    Test the describe function to ensure it outputs the schema correctly.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = extract()
    df = load_data(spark, data=path)

    # Capture describe output (no assertions since it's visual)
    describe(df)
    spark.stop()
