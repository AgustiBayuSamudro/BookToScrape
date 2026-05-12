import requests
from bs4 import BeautifulSoup
import json
from kafka import KafkaProducer
from datetime import datetime


def run_producer():

    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )

    url = "https://books.toscrape.com/"

    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        books = soup.find_all('article', class_='product_pod')

        print(f"Total books ditemukan: {len(books)}")

        for b in books:

            title = b.h3.a['title']
            price = b.find('p', class_='price_color').text
            rating = b.find('p', class_='star-rating')['class'][1]

            data = {
                "title": title,
                "price": price,
                "rating": rating,
                "scraped_at": datetime.now().isoformat()
            }

            producer.send(
                'books-topic',
                value=data
            )

            print(f"Data terkirim: {title}")

        producer.flush()

        print("Ingest Berhasil!")

    except Exception as e:
        print(f"Error saat ingest: {e}")

    finally:
        producer.close()


if __name__ == "__main__":
    run_producer()