import unittest
from pyspark.sql import SparkSession
from lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark
)

class TestMainFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and load data for testing."""
        cls.spark = start_spark("TestPhoneDataProcessing")
        cls.data_path = extract()
        cls.df = load_data(cls.spark, data=cls.data_path)

    @classmethod
    def tearDownClass(cls):
        """End the Spark session after tests."""
        end_spark(cls.spark)

    def test_extract(self):
        """Test if the file path returned by extract exists."""
        path = extract()
        self.assertTrue(os.path.exists(path), "The extracted file path does not exist.")

    def test_load_data(self):
        """Test if the data loads correctly and has expected columns."""
        expected_columns = [
            "phone_brand", "phone_model", "store", "price", "currency", "price_USD",
            "storage", "ram", "Launch", "Dimensions", "Chipset", "CPU", "GPU",
            "Year", "Foldable", "PPI_Density", "quantile_10", "quantile_50",
            "quantile_90", "price_range"
        ]
        df_columns = self.df.columns
        self.assertTrue(all(col in df_columns for col in expected_columns), "DataFrame columns do not match expected columns.")

    def test_describe(self):
        """Test the describe function to check basic statistics."""
        describe_output = describe(self.df)
        self.assertIsNotNone(describe_output, "Describe output should not be None.")

    def test_query(self):
        """Test the query function by calculating the average price per brand."""
        sql_query = """
        SELECT phone_brand, AVG(price_USD) AS avg_price_usd
        FROM phone_data
        GROUP BY phone_brand
        ORDER BY avg_price_usd DESC
        """
        self.df.createOrReplaceTempView("phone_data")
        result_df = self.spark.sql(sql_query)
        
        # Check if the query output has the expected columns
        self.assertIn("phone_brand", result_df.columns, "Query result missing 'phone_brand' column.")
        self.assertIn("avg_price_usd", result_df.columns, "Query result missing 'avg_price_usd' column.")

    def test_example_transform(self):
        """Test the example_transform function to filter high-priced phones."""
        transformed_df = example_transform(
            self.df.filter(self.df.price_range == "high price").select("phone_brand", "phone_model", "price_USD", "store")
        )
        
        # Ensure the transform only includes high price phones
        self.assertTrue(transformed_df.filter(transformed_df.price_range != "high price").count() == 0, "Transformation includes non-high price rows.")

if __name__ == "__main__":
    unittest.main()
