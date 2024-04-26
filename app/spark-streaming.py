from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, to_json, struct
from pyspark.sql.types import StringType, StructType, StructField
from textblob import TextBlob

spark_version = "3.5.1"  # Adjust this to match your Spark version
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"

def analyze_sentiment(text):
    if text is None or text.strip() == "":
        return None  # Or return a default sentiment value, e.g., 0.0
    return str(TextBlob(text).sentiment.polarity)

analyze_sentiment_udf = udf(analyze_sentiment, StringType())

spark = SparkSession \
    .builder \
    .appName("SentimentAnalysis") \
    .config("spark.jars.packages", kafka_package) \
    .getOrCreate()

schema = StructType([
    StructField("tweet", StringType()),
    StructField("user_id", StringType())
])

raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "RAW") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json")

parsed_df = raw_df.select(from_json("json", schema).alias("data"))
parsed_df.printSchema()  # Debug schema issues

data_df = parsed_df.select("data.*")

sentiment_df = data_df.withColumn("sentiment", analyze_sentiment_udf(col("tweet")))

# Create a JSON structure to send to Kafka
output_df = sentiment_df.select(
    to_json(struct(
        col("user_id"),
        col("tweet"),
        col("sentiment")
    )).alias("value")
)

kafka_query = output_df \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "SENTIMENT") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

# Print out the results to the console
query = sentiment_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

kafka_query.awaitTermination()
query.awaitTermination()
