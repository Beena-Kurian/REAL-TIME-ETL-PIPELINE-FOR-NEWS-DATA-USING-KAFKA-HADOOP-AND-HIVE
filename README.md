# REAL-TIME ETL PIPELINE FOR NEWS DATA USING KAFKA, HADOOP, AND HIVE

This project demonstrates a real-time ETL (Extract, Transform, Load) pipeline that collects, processes, and analyzes global news articles using a modern big data stack. The pipeline is built using Apache Kafka, Hadoop HDFS, and Apache Hive, and is powered by NLP techniques like Named Entity Recognition and Sentiment Analysis.

The system automatically fetches news articles from NewsAPI across categories such as politics, technology, finance, health, and sports. The Kafka producer script (news_producer.py) fetches the articles, applies NLP transformations using Python libraries like spaCy and VADER, and streams them to a Kafka topic named news-stream.

### Transformations include:

* Keyword and named entity extraction using spaCy

* Sentiment classification (Positive, Negative, Neutral) using VADER

* Adding metadata like publication time, reputation, and word count

The Kafka consumer (news_consumer.py) subscribes to the topic, processes and cleans the data, and writes it in structured JSON format to Hadoop HDFS.

Apache Hive is then used to define an external table over the HDFS-stored JSON files, allowing powerful analytical queries using HiveQL.

### Use Cases

* Tracking trending news topics

* Analyzing sentiment distribution over time

* Identifying the most reliable news sources

This project is ideal for students and professionals seeking hands-on experience with real-time big data pipelines. It demonstrates how open-source tools can be combined for scalable and efficient data processing.

## Project Workflow

### 1. Extract

Source: NewsAPI

Tool: news_producer.py

Function: Pulls articles and publishes to Kafka topic news-stream

### 2. Transform

Pre-Kafka (news_producer.py):

  * spaCy NER for entity extraction
  
  * VADER for sentiment

  * Reputation tagging, keyword extraction, word count

Post-Kafka (news_consumer.py):

  *  Cleansing, de-duplication, JSON formatting
    
  * Saves to HDFS at /BigData/news-stream/news_data.json

### 3. Load

* Storage: HDFS

* Hive Table: news_stream_db.news_articles

* Query Engine: Hive + JsonSerDe

## Setup Instructions

### Step 1: Install and Configure Zookeeper

wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.14/zookeeper-3.4.14.tar.gz
tar -xzf zookeeper-3.4.14.tar.gz
cd zookeeper-3.4.14/conf
cp zoo_sample.cfg zoo.cfg
nano zoo.cfg

Add at end of zoo.cfg:

dataDir=/var/zookeeper
server.0=127.0.0.1:2888:3888

sudo mkdir /var/zookeeper
sudo chown $USER:$USER /var/zookeeper
echo "0" > /var/zookeeper/myid
cd ..
bin/zkServer.sh start

### Step 2: Install and Configure Kafka

wget https://packages.confluent.io/archive/4.1/confluent-4.1.4-2.11.tar.gz
tar -xzf confluent-4.1.4-2.11.tar.gz
cd confluent-4.1.4
nano etc/kafka/server.properties

Ensure these lines:

broker.id=0
zookeeper.connect=localhost:2181

nano etc/kafka/zookeeper.properties
#### Ensure:
dataDir=/var/zookeeper

### Step 3: Start Kafka Broker

#### Foreground
bin/kafka-server-start etc/kafka/server.properties

#### Or background
nohup bin/kafka-server-start etc/kafka/server.properties > /dev/null 2>&1 &

#### Stop Kafka
bin/kafka-server-stop

### Step 4: Check Status

bin/zkServer.sh status
netstat -an | grep 9092

### Step 5: Python Environment Setup

pip install kafka-python newsapi-python spacy nltk
python -m nltk.downloader vader_lexicon
python -m spacy download en_core_web_sm

### Step 6: Create Kafka Topic (First-time Only)


bin/kafka-topics --create --topic news-stream --zookeeper localhost:2181 --partitions 1 --replication-factor 1

To list topics:
bin/kafka-topics --list --zookeeper localhost:2181

To delete a  topic:
bin/kafka-topics --delete --topic news-stream --zookeeper localhost:2181


### Step 7: Run Producer and Consumer Scripts

#### Terminal 1
Create folders to save results in HDFS:

hadoop fs -mkdir -p BigData/news-stream

#### Terminal 2
python news_producer.py

#### Terminal 3 
python news_consumer.py

#### Terminal 4 - HDFS Output Check  

hadoop fs -ls /BigData/news-stream
hadoop fs -cat /BigData/news-stream/news_data.json | head -n 3

#### Terminal 5 - Hive Table Setup

CREATE DATABASE news_stream_db;
USE news_stream_db;

CREATE EXTERNAL TABLE IF NOT EXISTS news_articles (
  source STRING,
  source_id STRING,
  category STRING,
  language STRING,
  country STRING,
  reputation STRING,
  author STRING,
  title STRING,
  description STRING,
  url STRING,
  year INT,
  month INT,
  day INT,
  word_count INT,
  keywords ARRAY<STRING>,
  sentiment STRING,
  polarity FLOAT,
  confidence FLOAT,
  fetchTime STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES ("ignore.malformed.json" = "true")
STORED AS TEXTFILE
LOCATION '/BigData/news-stream/';

-- If needed (each time you open hive)
ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core-3.1.3.jar;

#### Sample Hive Queries

-- Sentiment Distribution
SELECT sentiment, COUNT(*) AS total_articles FROM news_articles GROUP BY sentiment ORDER BY total_articles DESC;

-- Top 10 Keywords
SELECT keyword, COUNT(*) AS frequency FROM news_articles LATERAL VIEW explode(keywords) tmp AS keyword WHERE keyword != 'Unknown' GROUP BY keyword ORDER BY frequency DESC LIMIT 10;

-- Trusted Sources
SELECT source, COUNT(*) AS article_count FROM news_articles WHERE reputation = 'Trusted' GROUP BY source ORDER BY article_count DESC;

-- Top Authors
SELECT author, COUNT(*) AS article_count FROM news_articles WHERE author IS NOT NULL AND author != 'Unknown' GROUP BY author ORDER BY article_count DESC LIMIT 5;

### Future Scope
Use Spark for faster processing
Data Visualisations
Add dashboards 
Enhance sentiment model accuracy

