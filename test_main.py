import os
from pyspark.sql import SparkSession
from lib import (
    extract,
    load_data,
    query,
)


def test_extract():
    """
    Test if the extract function returns a valid path.
    """
    path = extract()
    assert os.path.exists(path), "Data path does not exist."


def test_load_data():
    """
    Test if data loads correctly into a DataFrame.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = extract()
    df = load_data(spark, data=path)
    assert df.count() > 0, "DataFrame is empty."
    spark.stop()


def test_query():
    """
    Test the query function with a sample SQL query.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = extract()
    df = load_data(spark, data=path)
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
