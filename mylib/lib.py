from pyspark.sql import SparkSession


def extract():
    """
    Returns the path to the dataset file.
    """
    return "processed_data_news.csv"


def start_spark(app_name):
    """
    Starts and returns a Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def load_data(spark, data):
    """
    Loads a CSV file into a Spark DataFrame.
    """
    return spark.read.csv(data, header=True, inferSchema=True)


def describe(df):
    """
    Prints the schema and a sample of the dataset.
    """
    df.printSchema()
    df.show(5)


def query(spark, df, sql_query, table_name):
    """
    Executes a SQL query on the provided DataFrame.
    """
    df.createOrReplaceTempView(table_name)
    result = spark.sql(sql_query)
    result.show()


def example_transform(df):
    """
    Prints the transformed DataFrame.
    """
    df.show(5)


def end_spark(spark):
    """
    Stops the Spark session.
    """
    spark.stop()
