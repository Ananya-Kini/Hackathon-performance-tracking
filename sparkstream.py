from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window,lit
from pyspark.sql.types import StructType, StringType, TimestampType
import time
from datetime import datetime


base_path = "/home/pes2ug22cs064/DBT_Project/jars"
jars = ",".join([
    f"{base_path}/spark-sql-kafka-0-10_2.13-3.5.0.jar",
    f"{base_path}/spark-token-provider-kafka-0-10_2.13-3.5.0.jar",
    f"{base_path}/kafka-clients-3.6.1.jar",
    f"{base_path}/commons-pool2-2.11.1.jar"
])
# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HackathonStreamingProcessor") \
    .master("local[*]") \
    .config("spark.jars", jars) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== Schemas for each event type =====
commit_schema = StructType() \
    .add("team_id", StringType()) \
    .add("user_id", StringType()) \
    .add("commit_msg", StringType()) \
    .add("timestamp", StringType())

submission_schema = StructType() \
    .add("team_id", StringType()) \
    .add("challenge_id", StringType()) \
    .add("status", StringType()) \
    .add("timestamp", StringType())

message_schema = StructType() \
    .add("team_id", StringType()) \
    .add("user_id", StringType()) \
    .add("message_text", StringType()) \
    .add("sentiment", StringType()) \
    .add("timestamp", StringType())

# ===== Kafka stream readers =====

# Commits
commits_raw = spark.readStream \
    .format("kafka") \
    .option("subscribe", "commits") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .load()

commits_df = commits_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), commit_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Submissions
submissions_raw = spark.readStream \
    .format("kafka") \
    .option("subscribe", "submissions") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .load()

submissions_df = submissions_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), submission_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Messages
messages_raw = spark.readStream \
    .format("kafka") \
    .option("subscribe", "messages") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .load()

messages_df = messages_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# ===== Transformations =====
commits_count = commits_df \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("team_id")
    ).count() \
    .withColumn("event_type", lit("commit"))


submissions_count = submissions_df \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("team_id")
    ).count() \
    .withColumn("event_type", lit("submission"))  

sentiment_count = messages_df \
    .groupBy(
        window(col("timestamp"), "1 minute", "30 seconds"),
        col("team_id"),
        col("sentiment")
    ).count() \
    .withColumn("event_type", lit("message_sentiment"))

def log_commit_batch(df, epoch_id):
    start = time.time()
    print(f"\n[Epoch {epoch_id}] ⏳ Processing commit batch...")
    df.show(truncate=False, n=50)
    end = time.time()
    print(f"✅ Commit batch {epoch_id} processed in {round(end - start, 3)} seconds")

commits_query = commits_count \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(log_commit_batch) \
    .start()

# Submissions
def log_submission_batch(df, epoch_id):
    start = time.time()
    print(f"\n[Epoch {epoch_id}] ⏳ Processing submission batch...")
    df.show(truncate=False, n=50)
    end = time.time()
    print(f"✅ Submission batch {epoch_id} processed in {round(end - start, 3)} seconds")

submissions_query = submissions_count \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(log_submission_batch) \
    .start()

# Sentiments
def log_sentiment_batch(df, epoch_id):
    start = time.time()
    print(f"\n[Epoch {epoch_id}] ⏳ Processing sentiment batch...")
    df.show(truncate=False, n=50)
    end = time.time()
    print(f"✅ Sentiment batch {epoch_id} processed in {round(end - start, 3)} seconds")

sentiment_query = sentiment_count \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(log_sentiment_batch) \
    .start()


# Await all
spark.streams.awaitAnyTermination()





