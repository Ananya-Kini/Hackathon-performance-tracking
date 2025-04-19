from pyspark.sql import SparkSession

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

# Register as temp views for SQL queries
df_commits.createOrReplaceTempView("commits")
df_messages.createOrReplaceTempView("messages")
df_submissions.createOrReplaceTempView("submissions")

# ===================== QUERIES ===================== #

# 1. Commit count per team
print("✅ Commit Count Per Team")
spark.sql("""
    SELECT team_id, COUNT(*) AS total_commits
    FROM commits
    GROUP BY team_id
    ORDER BY total_commits DESC
""").show(truncate=False)

# 2. Top contributors (users with most commits)
print("✅ Top Contributors")
spark.sql("""
    SELECT user_id, COUNT(*) AS commit_count
    FROM commits
    GROUP BY user_id
    ORDER BY commit_count DESC
""").show(truncate=False)

# 3. Submission frequency per team
print("✅ Submission Count Per Team")
spark.sql("""
    SELECT team_id, COUNT(*) AS submission_count
    FROM submissions
    GROUP BY team_id
    ORDER BY submission_count DESC
""").show(truncate=False)

# 4. Sentiment breakdown from messages
print("✅ Sentiment Distribution Per Team")
spark.sql("""
    SELECT team_id, sentiment, COUNT(*) AS count
    FROM messages
    GROUP BY team_id, sentiment
    ORDER BY team_id
""").show(truncate=False)

