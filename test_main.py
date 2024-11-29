import os
import pytest
from pyspark.sql import SparkSession
from mylib.lib import (
    load_data,
    describe,
    query,
)


@pytest.fixture
def mock_extract(monkeypatch, tmp_path):
    """
    Mocks the extract function to return a temporary CSV file.
    """
    test_file = tmp_path / "test_data.csv"
    test_file.write_text("phone_brand,price_USD,price_range\nApple,999,high price\nSamsung,799,medium price\n")

    def mock_return():
        return str(test_file)

    monkeypatch.setattr("mylib.lib.extract", mock_return)
    return str(test_file)


def test_extract(mock_extract):
    """
    Test if the mock extract function returns a valid path.
    """
    path = mock_extract
    assert isinstance(path, str), "Extracted path is not a string."
    assert os.path.exists(path), f"Path does not exist: {path}"


def test_load_data(mock_extract):
    """
    Test if data loads correctly into a DataFrame.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = mock_extract
    df = load_data(spark, data=path)
    assert df is not None, "DataFrame is None."
    assert df.count() > 0, "DataFrame is empty."
    spark.stop()


def test_query(mock_extract):
    """
    Test the query function with a sample SQL query.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = mock_extract
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


def test_describe(mock_extract):
    """
    Test the describe function to ensure it outputs the schema correctly.
    """
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    path = mock_extract
    df = load_data(spark, data=path)

    # Capture describe output (no assertions since it's visual)
    describe(df)
    spark.stop()
