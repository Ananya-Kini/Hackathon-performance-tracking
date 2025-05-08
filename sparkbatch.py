from pyspark.sql import SparkSession
import time

jdbc_url = "jdbc:postgresql://localhost:5432/dbt"
properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("HackathonBatchAnalysis") \
    .config("spark.jars", "/home/pes2ug22cs064/DBT_Project/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_commits = spark.read.jdbc(jdbc_url, "commits", properties=properties)
df_messages = spark.read.jdbc(jdbc_url, "messages", properties=properties)
df_submissions = spark.read.jdbc(jdbc_url, "submissions", properties=properties)

df_commits.createOrReplaceTempView("commits")
df_messages.createOrReplaceTempView("messages")
df_submissions.createOrReplaceTempView("submissions")


def timed_query(label, query):
    print(f"\nRunning {label}...")
    start = time.time()
    spark.sql(query).show(truncate=False)
    end = time.time()
    print(f"{label} completed in {round(end - start, 3)} seconds")

timed_query("Commit Count Per Team", """
    SELECT team_id, COUNT(*) AS total_commits
    FROM commits
    GROUP BY team_id
    ORDER BY total_commits DESC
""")

timed_query("Top Contributors", """
    SELECT user_id, COUNT(*) AS commit_count
    FROM commits
    GROUP BY user_id
    ORDER BY commit_count DESC
""")

timed_query("Submission Count Per Team", """
    SELECT team_id, COUNT(*) AS submission_count
    FROM submissions
    GROUP BY team_id
    ORDER BY submission_count DESC
""")

timed_query("Sentiment Distribution Per Team", """
    SELECT team_id, sentiment, COUNT(*) AS count
    FROM messages
    GROUP BY team_id, sentiment
    ORDER BY team_id
""")

