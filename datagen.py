import time, random
from kafka import KafkaProducer
import json
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

teams = [f'Team{i}' for i in range(1, 11)]

team_users = {
    team: [f'User{j}_{team}' for j in range(1, 4)]
    for team in teams
}

challenges = ['Open Innovation', 'Framework', 'FOSS Track']

while True:
    event_type = random.choice(['commit', 'message', 'submission'])
    team = random.choice(teams)
    user = random.choice(team_users[team])

    if event_type == 'commit':
        data = {
            "team_id": team,
            "user_id": user,
            "commit_msg": random.choice(["Initial commit", "Fixed bug", "Added feature"]),
            "timestamp": datetime.now().isoformat()
        }
        topic = 'commits'

    elif event_type == 'message':
        data = {
            "team_id": team,
            "user_id": user,
            "message_text": random.choice(["Great job!", "Need help", "Almost done"]),
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "timestamp": datetime.now().isoformat()
        }
        topic = 'messages'

    else:
        data = {
            "team_id": team,
            "track": random.choice(challenges),
            "status": random.choice(["submitted", "in-progress", "failed"]),
            "timestamp": datetime.now().isoformat()
        }
        topic = 'submissions'

    producer.send(topic, value=data)
    print(f"Sent to {topic}: {data}")
    time.sleep(2)

