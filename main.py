"""
Main CLI or app entry point
"""

from lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def main():
    # Extract data path
    data_path = extract()
    
    # Start Spark session
    spark = start_spark("PhoneDataProcessing")
    
    # Load data into DataFrame
    df = load_data(spark, data=data_path)
    
    # Describe the data (example metric)
    describe(df)
    
    # SQL Query: Calculate average price in USD per brand
    query(
        spark,
        df,
        """
        SELECT phone_brand, AVG(price_USD) AS avg_price_usd
        FROM phone_data
        GROUP BY phone_brand
        ORDER BY avg_price_usd DESC
        """,
        "phone_data"
    )
    
    # Example Transform: Filter high-priced phones and select relevant columns
    example_transform(
        df.filter(df.price_range == "high price").select("phone_brand", "phone_model", "price_USD", "store")
    )
    
    # End Spark session
    end_spark(spark)


if __name__ == "__main__":
    main()
