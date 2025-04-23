from kafka import KafkaConsumer
import json
import os
import re
import subprocess
import time
from datetime import datetime

topic = "news-stream"
brokers = "localhost:9092"
group_id = "news-consumer-group"

# Initialize Kafka Consumer
def safe_json_deserializer(value):
    """Ensure JSON is properly deserialized from bytes."""
    try:
        if isinstance(value, bytes):
            return json.loads(value.decode('utf-8'))
        return value  
    except json.JSONDecodeError:
        print(f"ERROR: Failed to decode JSON: {value}")
        return None 

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=brokers,
    group_id=group_id,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=safe_json_deserializer
)

HDFS_PATH = "/BigData/news-stream/news_data.json"

# Ensure HDFS directory exists
try:
    os.system(f"hadoop fs -mkdir -p {os.path.dirname(HDFS_PATH)}")
except Exception as e:
    print(f"ERROR: Failed to create HDFS directory: {e}")

# Track processed articles to avoid duplicates
processed_articles = set()

def clean_data(record):
    """Clean and structure JSON records."""
    try:
        # Avoid returning None
        if not record:
            return ""  

        # Use URL as a unique identifier to prevent duplicates
        article_id = record.get("url", "")
        if not article_id or article_id in processed_articles:
            return ""  

        processed_articles.add(article_id)

        # Validate required fields 
        if not record.get("title") or not record.get("url") or not record.get("publishedAt"):
            return "" 

        # Convert 'publishedAt' to year, month, day
        try:
            date_obj = datetime.strptime(record["publishedAt"], "%Y-%m-%dT%H:%M:%SZ")
            year, month, day = date_obj.year, date_obj.month, date_obj.day
        except ValueError:
            return 

        # Keep only required fields
        cleaned_data = {
            "source": record.get("source", {}).get("name", "Unknown"),
            "source_id": record.get("source", {}).get("id", "Unknown"),
            "category": record.get("source", {}).get("category", "Unknown"),
            "language": record.get("source", {}).get("language", "Unknown"),
            "country": record.get("source", {}).get("country", "Unknown"),
            "reputation": record.get("source", {}).get("reputation", "Unknown"),
            "author": record.get("author", "Unknown"),
            "title": record["title"],
            "description": record.get("description", "No Description"),
            "url": record["url"],
            "year": year,
            "month": month,
            "day": day,
            "word_count": record.get("word_count", 0),
            "keywords": record.get("keywords", ["Unknown"]),
            "sentiment": record.get("sentiment", "Neutral"),
            "polarity": record.get("polarity", 0.0),
            "confidence": record.get("confidence", 0.0),
            "fetchTime": record.get("fetchTime", "Unknown")
        }

        print(f"PROCESSED: {record['publishedAt']} | {record['title']}")
        return json.dumps(cleaned_data, ensure_ascii=False)

    except Exception as e:
        print(f"ERROR: Data Cleaning Failed: {e}")
        return ""

def write_to_hdfs(cleaned_record):
    """Write a single cleaned record directly to HDFS."""
    # Skip empty records
    if not cleaned_record:
        return  

    try:
        process = subprocess.run(
            ["hadoop", "fs", "-appendToFile", "-", HDFS_PATH],
            input=cleaned_record + "\n",
            text=True,
            check=True
        )
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: HDFS Write Failed: {e}")

print(f"Listening for new messages on topic: {topic}")

for message in consumer:
    try:
        article = message.value
        # Skip invalid messages
        if article is None:
            continue  

        cleaned_record = clean_data(article)
        if cleaned_record:
            write_to_hdfs(cleaned_record)

        consumer.commit_async()

    except Exception as e:
        print(f"ERROR: Exception in processing loop: {e}")