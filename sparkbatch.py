from pyspark.sql import SparkSession
import time

# JDBC config
jdbc_url = "jdbc:postgresql://localhost:5432/dbt"
properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Spark session
spark = SparkSession.builder \
    .appName("HackathonBatchAnalysis") \
    .config("spark.jars", "/home/pes2ug22cs064/DBT_Project/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load tables into DataFrames
df_commits = spark.read.jdbc(jdbc_url, "commits", properties=properties)
df_messages = spark.read.jdbc(jdbc_url, "messages", properties=properties)
df_submissions = spark.read.jdbc(jdbc_url, "submissions", properties=properties)

# Register as temp views
df_commits.createOrReplaceTempView("commits")
df_messages.createOrReplaceTempView("messages")
df_submissions.createOrReplaceTempView("submissions")

# ===================== QUERIES WITH TIMING ===================== #

def timed_query(label, query):
    print(f"\n⏳ Running {label}...")
    start = time.time()
    spark.sql(query).show(truncate=False)
    end = time.time()
    print(f"{label} completed in {round(end - start, 3)} seconds")

# 1. Commit count per team
timed_query("Commit Count Per Team", """
    SELECT team_id, COUNT(*) AS total_commits
    FROM commits
    GROUP BY team_id
    ORDER BY total_commits DESC
""")

# 2. Top contributors
timed_query("Top Contributors", """
    SELECT user_id, COUNT(*) AS commit_count
    FROM commits
    GROUP BY user_id
    ORDER BY commit_count DESC
""")

# 3. Submission frequency per team
timed_query("Submission Count Per Team", """
    SELECT team_id, COUNT(*) AS submission_count
    FROM submissions
    GROUP BY team_id
    ORDER BY submission_count DESC
""")

# 4. Sentiment breakdown
timed_query("Sentiment Distribution Per Team", """
    SELECT team_id, sentiment, COUNT(*) AS count
    FROM messages
    GROUP BY team_id, sentiment
    ORDER BY team_id
""")

