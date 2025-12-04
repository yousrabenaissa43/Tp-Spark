from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("SeattleWeatherStreaming") \
    .getOrCreate()

# Define schema of the incoming JSON
schema = StructType([
    StructField("date", StringType(), True),
    StructField("precipitation", FloatType(), True),
    StructField("temp_max", FloatType(), True),
    StructField("temp_min", FloatType(), True),
    StructField("wind", FloatType(), True),
    StructField("weather", StringType(), True)
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka stores values as bytes, so convert to string
json_df = df.selectExpr("CAST(value AS STRING) as json_str")

# Parse JSON
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

# Example: Calculate average temp_max and temp_min per weather type (streaming aggregation)
agg_df = parsed_df.groupBy("weather").avg("temp_max", "temp_min")

# Write results to PostgreSQL
def foreach_batch_function(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres-db:5432/weather_db") \
        .option("dbtable", "weather_summary") \
        .option("user", "postgres") \
        .option("password", "passw0rd") \
        .mode("append") \
        .save()

query = agg_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
