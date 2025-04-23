from kafka import KafkaProducer
from newsapi import NewsApiClient
import json
import time
import spacy
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Initialize VADER for sentiment analysis
vader = SentimentIntensityAnalyzer()

# List of API keys to handle rate limits(please replace Key1-4 with your api keys)
API_KEYS = ["key-1","key-2",
            "key-3","key-4"]

# Track current API key index
current_api_index = 0
newsapi = NewsApiClient(api_key=API_KEYS[current_api_index])

# Load spaCy NLP model for Named Entity Recognition (NER)
nlp = spacy.load("en_core_web_sm")

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Retrieve available news sources from NewsAPI
def get_news_sources():
    try:
        response = newsapi.get_sources()
        if response.get("status") == "ok":
            return {source["id"]: source for source in response.get("sources", [])}
    except Exception as e:
        print(f"Error fetching sources: {e}")
    return {}

# Store source metadata in a dictionary
news_sources = get_news_sources()

# Trusted News Sources for Reputation Classification
trusted_sources = [
    # International & General News
    "BBC News", "CNN", "The Guardian","The New York Times","The Washington Post",
    "Associated Press","Reuters", "Al Jazeera English",
    # Business & Finance
    "Bloomberg","The Wall Street Journal","Financial Times","Business Insider",
    # Technology
    "TechCrunch","The Verge","Wired","Ars Technica","Engadget",
    # Science & Public Media
    "NPR", "New Scientist", 
    # Regional News 
    "The Hindu",  # India
    "CBC News","The Globe and Mail",  # Canada
    "Politico","The Hill",  # U.S.
]

# Switch to the next API key if rate limit is reached
def switch_api_key():
    global current_api_index, newsapi
    current_api_index = (current_api_index + 1) % len(API_KEYS)
    print(f"Switching to API Key: {API_KEYS[current_api_index]}")
    newsapi = NewsApiClient(api_key=API_KEYS[current_api_index])

# fetch news articles
def fetch_news():
    try:
        response = newsapi.get_everything(
            q = "technology OR finance OR politics OR sports OR health OR environment OR science",
            language="en",
            sort_by="publishedAt",
            page_size=100  # Fetch max allowed articles per request        )
            )
        if response.get("status") == "error" and "maximum" in response.get("message", "").lower():
            print("API Limit reached. Switching keys...")
            switch_api_key()
            return fetch_news()  # Retry with new API key

        articles = response.get("articles", [])
        for article in articles:
            send_to_kafka(process_article(article))  # Process and send to Kafka

    except Exception as e:
        print(f"Fetching news failed: {e}")


# Process Each Article
def process_article(article):
    source_info = article.get("source", {})
    source_id = source_info.get("id", "Unknown")

    # Ensure title, description, and content are strings before concatenation
    title = str(article.get("title", ""))
    description = str(article.get("description", ""))
    content = str(article.get("content", ""))

    # detected_keywords = detect_keywords(title + " " + description + " "+ content)
    detected_keywords = detect_keywords(title + " " + description + " " + content)
    sentiment_data = analyze_sentiment(title, content)
    word_count = count_words(content)

    # Retrieve additional source metadata if available
    source_metadata = news_sources.get(source_id, {})
    source_category = source_metadata.get("category", "Unknown")
    source_language = source_metadata.get("language", "Unknown")
    source_country = source_metadata.get("country", "Unknown")

    # Extracting detailed news data
    formatted_article = {
        "source": {
            "id": source_id,
            "name": source_info.get("name", "Unknown"),
            "category": source_category,
            "language": source_language,
            "country": source_country,
            "reputation": "Trusted" if source_info.get("name", "") in trusted_sources else "Untrusted"
        },
        "author": article.get("author", "Unknown"),
        "title": title if title else "No Title",
        "description": description if description else "No Description",
        "url": article.get("url"),
        "publishedAt": article.get("publishedAt", "Unknown Date"),
        "content": content,
        "word_count": word_count,
        "keywords": detected_keywords,
        "sentiment": sentiment_data["sentiment"],
        "polarity": sentiment_data["polarity"],
        "confidence": sentiment_data["confidence"],
        "fetchTime": time.strftime("%Y-%m-%d %H:%M:%S")
    }

    return formatted_article

# Runs spaCy NLP Pipeline
def detect_keywords(text):
    # Prevents processing empty text
    if not text.strip():  
        return ["Unknown"]
    doc = nlp(text[:1000])  
    # extracts Named Entities
    named_entities = [ent.text for ent in doc.ents if ent.label_ in {"ORG", "GPE", "PERSON"}]
    return named_entities if named_entities else ["Unknown"]

# Analyze Sentiment using VADER
def analyze_sentiment(title, content):
    combined_text = f"{title}. {content}"
    # VADER analyzes sentiment
    vader_scores = vader.polarity_scores(combined_text) 
    polarity = vader_scores["compound"]
    sentiment_label = (
        "Positive" if polarity > 0.2 else
        "Negative" if polarity < -0.2 else
        "Neutral"
    )
    return {"sentiment": sentiment_label, "polarity": polarity, "confidence": vader_scores["pos"]}

# Count words in content
def count_words(text):
    return len(text.split()) if text else 0

# Send Data to Kafka
def send_to_kafka(article, topic="news-stream"):
    try:
        producer.send(topic, article)
        print(f"Sent: {article['title']} | Keywords: {', '.join(article['keywords'])} | Sentiment: {article['sentiment']} | Word Count: {article['word_count']}")
        producer.flush()
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

# Main Execution
if __name__ == "__main__":
    while True:
        fetch_news()
        print("Waiting for 60 seconds before fetching again...")
        time.sleep(60)