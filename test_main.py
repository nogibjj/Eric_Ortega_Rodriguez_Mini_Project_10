import os
import pytest
from pyspark.sql import SparkSession
from mylib.lib import load_data, describe, query


@pytest.fixture
def mock_extract(monkeypatch, tmp_path):
    """Mock the extract function to return a temporary test CSV file."""
    test_file = tmp_path / "test_data.csv"
    test_file.write_text(
        "phone_brand,price_USD,price_range\n"
        "Apple,999,high price\n"
        "Samsung,799,medium price\n"
    )
    monkeypatch.setattr("mylib.lib.extract", lambda: str(test_file))
    return str(test_file)


def test_extract(mock_extract):
    path = mock_extract
    assert os.path.exists(path), f"Path does not exist: {path}"


def test_load_data(mock_extract):
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    df = load_data(spark, data=mock_extract)
    assert df and df.count() > 0, "DataFrame is empty or None."
    spark.stop()


def test_query(mock_extract):
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    df = load_data(spark, data=mock_extract)
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
    spark = SparkSession.builder.appName("TestSession").getOrCreate()
    df = load_data(spark, data=mock_extract)
    describe(df)
    spark.stop()
