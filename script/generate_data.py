import csv
from io import BytesIO
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker
from flows.config import BUCKET_SOURCES

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_client(n_clients: int, output_path: str) -> list[int]:
  countries = [
    "France", "Germany", "Spain", "Italy", "Belgium",
    "Netherlands", "Switzerland", "UK", "Canada", "USA"
  ]

  clients = []
  client_ids = []

  for i in range(1, n_clients + 1):
    subscription_date = fake.date_between(start_date="-3y", end_date="-1m")
    clients.append(
      {
        "id_client": i,
        "name": fake.name(),
        "email": fake.email(),
        "subscription_date": subscription_date.strftime("%Y-%m-%d"),
        "country": random.choice(countries)
      }
    )

    client_ids.append(i)

  Path(output_path).parent.mkdir(parents=True, exist_ok=True)

  with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["id_client", "name", "email", "subscription_date", "country"])
    writer.writeheader()
    writer.writerows(clients)

  print(f"Generated {n_clients} clients in {output_path}")
  return client_ids

def generate_purchases(client_ids: list[int], avg_purchases_per_client: int, output_path: str) -> None:
  products = [
    "Laptop", "Phone", "Tablet", "Headphones", "Monitor", 
    "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"
  ]
  
  price_ranges = {
    "Laptop": (500, 2000),
    "Phone": (300, 1200),
    "Tablet": (200, 800),
    "Headphones": (50, 300),
    "Monitor": (150, 600),
    "Keyboard": (30, 150),
    "Mouse": (20, 100),
    "Webcam": (40, 200),
    "Speaker": (50, 300),
    "Charger": (10, 50)
  }
  
  purchases = []
  purchase_id = 1
  
  for client_id in client_ids:
    n_purchases = random.randint(
      max(1, avg_purchases_per_client - 3),
      avg_purchases_per_client + 3
    )
    
    for _ in range(n_purchases):
      product = random.choice(products)
      min_price, max_price = price_ranges[product]
      price = round(random.uniform(min_price, max_price), 2)
      quantity = random.randint(1, 5)
      purchase_date = fake.date_between(start_date="-2y", end_date="today")
      
      purchases.append({
        "id_purchase": purchase_id,
        "id_client": client_id,
        "product": product,
        "price": price,
        "quantity": quantity,
        "purchase_date": purchase_date.strftime("%Y-%m-%d"),
        "total": round(price * quantity, 2)
      })
      
      purchase_id += 1
  
  Path(output_path).parent.mkdir(parents=True, exist_ok=True)
  
  with open(output_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
      f, 
      fieldnames=["id_purchase", "id_client", "product", "price", "quantity", "purchase_date", "total"]
    )
    writer.writeheader()
    writer.writerows(purchases)
  
  print(f"Generated {len(purchases)} purchases in {output_path}")

if __name__ == "__main__":
  n_clients = 100
  output_path = "data/clients.csv"
  client_ids = generate_client(n_clients, output_path)
  
  avg_purchases_per_client = 5
  output_path = "data/purchases.csv"
  generate_purchases(client_ids, avg_purchases_per_client, output_path)
