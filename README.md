## Comparative Analysis of Streaming and Batch Processing on Real-Time Hackathon Data
This project analyzes the differences between **streaming** and **batch processing** techniques using **Apache Kafka** and **Apache Spark**. The system simulates a real-time hackathon environment by generating synthetic data (e.g., commits, messages, and submissions), then processes it using both real-time and batch pipelines to evaluate metrics and participation of the teams.

## System Architecture
![alt text](<dbt.drawio (2).png>)
- **Synthetic Data Generator**: Simulates live hackathon activity and sends data to Kafka topics.
- **Kafka**: Acts as a distributed message broker for decoupling data generation from processing.
- **Spark Streaming**: Consumes Kafka data in real-time for immediate analysis.
- **Spark Batch**: Periodically processes accumulated data for trend analysis.
- **Streamlit Dashboard**: Visualizes batch metrics.

## Steps to run
1. Start streaming using ```python3 sparkstream.py```
2. Run the Kafka consumer ```python3 consumer.py```
3. Start generating data using ```python3 datagen.py```
4. Microbatches for calculation of submission count, commit count and message sentiment per team keep running. The consumer just keeps adding the records to a PostgreSQL database(mentioned in dbdef.txt)
5. Run ```python3 sparkbatch.py``` whenever enough data is accumulated, it returns submission count, commit count and message sentiment per team and top contributors. 
6. Finally, run ```streamlit run app.py``` to see plots/visualization of the calculated stats. 