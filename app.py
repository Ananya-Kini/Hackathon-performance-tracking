import streamlit as st
import pandas as pd
import psycopg2
import seaborn as sns
import matplotlib.pyplot as plt

@st.cache_data
def load_data():
    conn = psycopg2.connect(
        dbname="dbt",
        user="postgres",
        password="password",
        host="localhost",
        port="5432"
    )

    df_commits = pd.read_sql("SELECT * FROM commits", conn)
    df_messages = pd.read_sql("SELECT * FROM messages", conn)
    df_submissions = pd.read_sql("SELECT * FROM submissions", conn)
    conn.close()
    return df_commits, df_messages, df_submissions

df_commits, df_messages, df_submissions = load_data()

st.title("📊 Hackathon Analytics Dashboard")

st.sidebar.title("Select View")
view = st.sidebar.radio("Choose a metric", ["Commits", "Submissions", "Sentiment", "Top Contributors"])

if view == "Commits":
    st.subheader("Commits Per Team")
    commits_summary = df_commits.groupby("team_id").size().reset_index(name="commit_count")
    fig, ax = plt.subplots()
    sns.barplot(data=commits_summary, x="team_id", y="commit_count", ax=ax)
    st.pyplot(fig)

elif view == "Submissions":
    st.subheader("Submissions Per Team")
    subs_summary = df_submissions.groupby("team_id").size().reset_index(name="submission_count")
    fig, ax = plt.subplots()
    sns.barplot(data=subs_summary, x="team_id", y="submission_count", ax=ax)
    st.pyplot(fig)

elif view == "Sentiment":
    st.subheader("Sentiment Distribution")
    sentiment_summary = df_messages.groupby(["team_id", "sentiment"]).size().reset_index(name="count")
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(data=sentiment_summary, x="team_id", y="count", hue="sentiment", ax=ax)
    st.pyplot(fig)

elif view == "Top Contributors":
    st.subheader("Top Contributors by Commit Count")
    top_users = df_commits.groupby("user_id").size().reset_index(name="commit_count").sort_values(by="commit_count", ascending=False)
    st.dataframe(top_users)


