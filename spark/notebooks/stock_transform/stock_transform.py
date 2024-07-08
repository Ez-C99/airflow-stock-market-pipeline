# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StructType, StructField, StringType, FloatType
import os
import json

if __name__ == '__main__':

    def app():
        # Create a SparkSession
        spark = SparkSession.builder.appName("FormatStock") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .getOrCreate()

        # Read the JSON file from Minio bucket
        json_data = spark.read.text(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/prices.json").collect()[0][0]
        data = json.loads(json_data)

        # Extract the time series data
        time_series = data["Time Series (Daily)"]

        # Define schema for structured parsing
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("open", FloatType(), True),
            StructField("high", FloatType(), True),
            StructField("low", FloatType(), True),
            StructField("close", FloatType(), True),
            StructField("volume", StringType(), True)
        ])

        rows = [(date, float(values["1. open"]), float(values["2. high"]), float(values["3. low"]), float(values["4. close"]), values["5. volume"]) for date, values in time_series.items()]
        df = spark.createDataFrame(rows, schema)

        # Convert date string to DateType
        df = df.withColumn('date', col('date').cast(DateType()))

        # Store in Minio
        df.write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("delimiter", ",") \
            .csv(f"s3a://{os.getenv('SPARK_APPLICATION_ARGS')}/formatted_prices")

    app()
    os.system('kill %d' % os.getpid())