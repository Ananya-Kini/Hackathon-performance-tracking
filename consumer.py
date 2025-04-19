from kafka import KafkaConsumer
import json
from datetime import datetime
import psycopg2

# PostgreSQL connection config
conn = psycopg2.connect(
    dbname="dbt",
    user="postgres",
    password="password",  
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# Kafka consumer
consumer = KafkaConsumer(
    'commits', 'submissions', 'messages',
    bootstrap_servers='localhost:9092',
    group_id='all_topics_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest'
)

print("Listening to all topics and inserting into PostgreSQL...")

for message in consumer:
    topic = message.topic
    data = message.value
    print(f"[{datetime.now().isoformat()}] {topic} => {data}")

    try:
        if topic == 'commits':
            cur.execute("""
                INSERT INTO commits (team_id, user_id, commit_msg, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (
                data.get("team_id"),
                data.get("user_id"),
                data.get("commit_msg"),
                data.get("timestamp")
            ))

        elif topic == 'messages':
            cur.execute("""
                INSERT INTO messages (team_id, user_id, message_text, sentiment, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                data.get("team_id"),
                data.get("user_id"),
                data.get("message_text"),
                data.get("sentiment"),
                data.get("timestamp")
            ))

        elif topic == 'submissions':
            cur.execute("""
                INSERT INTO submissions (team_id, challenge_id, status, timestamp)
                VALUES (%s, %s, %s, %s)
            """, (
                data.get("team_id"),
                data.get("challenge_id"),
                data.get("status"),
                data.get("timestamp")
            ))

        conn.commit()

    except Exception as e:
        print(f"Error inserting into {topic}: {e}")
        conn.rollback()

# Clean up (optional if you interrupt)
cur.close()
conn.close()

