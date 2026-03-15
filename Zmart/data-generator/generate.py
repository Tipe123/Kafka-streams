#!/usr/bin/env python3
"""
ZMart Kafka Data Generator
==========================
Produces realistic synthetic purchase events to the ZMart Kafka topics.

Two phases:
  1. Seed stores  → zmart.stores          (compacted, keyed by storeId)
  2. Continuous  → zmart.purchases.raw   (event stream, keyed by customerId)

Usage
-----
  # Install dependencies once
  pip install -r requirements.txt

  # Seed stores first, then stream 1 purchase/sec indefinitely
  python generate.py

  # Stream faster (5 purchases/sec) with more customers
  python generate.py --rate 5 --customers 50

  # Only re-seed the stores topic (e.g. after a topic reset)
  python generate.py --mode stores-only

  # Only stream purchases (stores already seeded)
  python generate.py --mode purchases-only --rate 2

  # Point at a different Kafka cluster
  python generate.py --bootstrap-servers broker1:9092,broker2:9092

Why these two topics?
---------------------
  zmart.stores        → GlobalKTable in the topology. Must be seeded BEFORE
                        purchases arrive, otherwise the join in step [7] of
                        ZMartTopology will find no match and produce nothing
                        to zmart.purchases.enriched.

  zmart.purchases.raw → The primary input. ZMartTopology reads this and fans
                        out to all downstream topics (masked, rewards, patterns,
                        department branches, enriched).
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer

# ─────────────────────────────────────────────────────────────────────────────
# Topic names — must match ZMartTopics.java exactly
# ─────────────────────────────────────────────────────────────────────────────
TOPIC_PURCHASES_RAW = "zmart.purchases.raw"
TOPIC_STORES        = "zmart.stores"

# ─────────────────────────────────────────────────────────────────────────────
# Reference data — mirrors what ZMartTopology expects
# ─────────────────────────────────────────────────────────────────────────────

DEPARTMENTS = ["ELECTRONICS", "CLOTHING", "FOOD", "TOOLS", "SPORTS", "BOOKS"]

ITEMS_BY_DEPT = {
    "ELECTRONICS": ["Wireless Headphones", "USB-C Hub", "Mechanical Keyboard",
                    "Monitor 27\"", "Webcam HD", "SSD 1TB", "Smart Speaker"],
    "CLOTHING":    ["Running Shoes", "Denim Jacket", "Cotton T-Shirt",
                    "Winter Coat", "Yoga Pants", "Sneakers"],
    "FOOD":        ["Organic Coffee", "Protein Bars", "Olive Oil",
                    "Greek Yogurt Pack", "Almond Milk"],
    "TOOLS":       ["Cordless Drill", "Screwdriver Set", "Measuring Tape",
                    "Level Tool", "Utility Knife"],
    "SPORTS":      ["Yoga Mat", "Dumbbells Set", "Resistance Bands",
                    "Water Bottle", "Jump Rope"],
    "BOOKS":       ["Clean Code", "Kafka: The Definitive Guide",
                    "Designing Data-Intensive Applications", "The Pragmatic Programmer"],
}

PRICE_RANGES = {
    "ELECTRONICS": (15.00, 599.99),
    "CLOTHING":    (9.99,  199.99),
    "FOOD":        (2.50,  49.99),
    "TOOLS":       (5.99,  249.99),
    "SPORTS":      (4.99,  159.99),
    "BOOKS":       (9.99,  54.99),
}

# Stores must match the storeIds that purchases will reference.
# Each store becomes one record in the zmart.stores compacted topic.
STORES = [
    {"storeId": "store-NYC-05", "storeName": "ZMart New York Midtown",
     "city": "New York",     "region": "EAST",  "country": "US",
     "address": "350 5th Ave, New York, NY 10118", "employeeCount": 142},

    {"storeId": "store-LA-12",  "storeName": "ZMart Los Angeles Downtown",
     "city": "Los Angeles",  "region": "WEST",  "country": "US",
     "address": "633 W 5th St, Los Angeles, CA 90071", "employeeCount": 98},

    {"storeId": "store-CHI-03", "storeName": "ZMart Chicago Loop",
     "city": "Chicago",      "region": "CENTRAL", "country": "US",
     "address": "77 W Wacker Dr, Chicago, IL 60601", "employeeCount": 115},

    {"storeId": "store-MIA-08", "storeName": "ZMart Miami Beach",
     "city": "Miami",        "region": "SOUTH", "country": "US",
     "address": "1601 Washington Ave, Miami Beach, FL 33139", "employeeCount": 87},

    {"storeId": "store-SEA-02", "storeName": "ZMart Seattle Capitol Hill",
     "city": "Seattle",      "region": "WEST",  "country": "US",
     "address": "400 Pine St, Seattle, WA 98101", "employeeCount": 76},
]

# ZIP codes — one per store city, used in PurchasePattern analytics
ZIP_BY_STORE = {
    "store-NYC-05": "10001",
    "store-LA-12":  "90001",
    "store-CHI-03": "60601",
    "store-MIA-08": "33101",
    "store-SEA-02": "98101",
}


# ─────────────────────────────────────────────────────────────────────────────
# Faker — generates realistic names, credit card numbers, etc.
# ─────────────────────────────────────────────────────────────────────────────
fake = Faker("en_US")

# Pre-generate a stable pool of customers so that the same customerId repeats
# (this makes the rewards aggregation interesting — you'll see points accumulate)
def build_customer_pool(size: int) -> list[str]:
    return [f"cust-{str(i).zfill(4)}" for i in range(1, size + 1)]


def build_employee_pool(size: int = 20) -> list[str]:
    return [f"emp-{str(i).zfill(3)}" for i in range(1, size + 1)]


# ─────────────────────────────────────────────────────────────────────────────
# Event builders
# ─────────────────────────────────────────────────────────────────────────────

def build_purchase_event(customers: list[str], employees: list[str]) -> dict:
    """
    Build a single raw purchase event as a Python dict.
    This mirrors the Purchase.java domain model exactly.
    """
    customer_id  = random.choice(customers)
    store        = random.choice(STORES)
    store_id     = store["storeId"]
    department   = random.choices(
        DEPARTMENTS,
        # Weighted so FOOD and ELECTRONICS appear more often — realistic
        weights=[25, 20, 30, 10, 10, 5],
        k=1
    )[0]
    item         = random.choice(ITEMS_BY_DEPT[department])
    quantity     = random.randint(1, 4)
    lo, hi       = PRICE_RANGES[department]
    unit_price   = round(random.uniform(lo, hi), 2)
    total        = round(unit_price * quantity, 2)

    return {
        "purchaseId":     str(uuid.uuid4()),
        "customerId":     customer_id,
        "storeId":        store_id,
        "employeeId":     random.choice(employees),
        "department":     department,
        "itemPurchased":  item,
        "quantity":       quantity,
        "price":          unit_price,
        "purchaseTotal":  total,
        # Raw credit card — will be masked by ZMartTopology step [1]
        "creditCardNumber": fake.credit_card_number(card_type="visa"),
        "zipCode":        ZIP_BY_STORE[store_id],
        # ISO-8601 timestamp — Jackson's JavaTimeModule will parse this to Instant
        "purchaseDate":   datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Kafka delivery callback — called once per message (async)
# ─────────────────────────────────────────────────────────────────────────────

def on_delivery(err, msg):
    if err:
        print(f"  [ERROR] Delivery failed for {msg.topic()}: {err}")
    else:
        print(f"  [OK]    topic={msg.topic():<32} "
              f"partition={msg.partition()} offset={msg.offset()} "
              f"key={msg.key().decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# Seeding: write all stores to zmart.stores
# ─────────────────────────────────────────────────────────────────────────────

def seed_stores(producer: Producer):
    """
    Publish every store record to the compacted zmart.stores topic.

    Key   = storeId  (e.g. "store-NYC-05")
    Value = full Store JSON

    The GlobalKTable in ZMartTopology reads this topic at startup and on
    every update. Because the topic is compacted, only the latest record
    per key is retained — perfect for slowly-changing reference data.

    You only need to run this once (or whenever store data changes).
    """
    print(f"\n{'='*60}")
    print(f"  Seeding {len(STORES)} stores → {TOPIC_STORES}")
    print(f"{'='*60}")

    for store in STORES:
        key   = store["storeId"]
        value = json.dumps(store)
        producer.produce(
            topic    = TOPIC_STORES,
            key      = key.encode("utf-8"),
            value    = value.encode("utf-8"),
            callback = on_delivery,
        )

    # flush() blocks until all pending messages are acknowledged
    producer.flush()
    print(f"\n  ✓ All {len(STORES)} stores seeded.\n")


# ─────────────────────────────────────────────────────────────────────────────
# Streaming: continuously produce purchases
# ─────────────────────────────────────────────────────────────────────────────

def stream_purchases(producer: Producer,
                     rate: float,
                     customers: list[str],
                     employees: list[str]):
    """
    Continuously produce purchase events to zmart.purchases.raw.

    Key   = customerId  — ensures all purchases by the same customer land
                          on the same partition (important for the rewards
                          aggregate in ZMartTopology step [6]).
    Value = full Purchase JSON (contains raw credit card — masked in topology)

    Rate  = events per second (can be fractional, e.g. 0.5 = one every 2s)
    """
    interval = 1.0 / rate
    count    = 0

    print(f"{'='*60}")
    print(f"  Streaming purchases → {TOPIC_PURCHASES_RAW}")
    print(f"  Rate     : {rate} msg/sec (interval = {interval:.2f}s)")
    print(f"  Customers: {len(customers)} unique IDs")
    print(f"  Press Ctrl+C to stop.")
    print(f"{'='*60}\n")

    try:
        while True:
            event = build_purchase_event(customers, employees)
            key   = event["customerId"]
            value = json.dumps(event)

            producer.produce(
                topic    = TOPIC_PURCHASES_RAW,
                key      = key.encode("utf-8"),
                value    = value.encode("utf-8"),
                callback = on_delivery,
            )

            count += 1
            # poll() serves the delivery callbacks — call it frequently
            producer.poll(0)

            # Every 10 messages flush to ensure they're sent
            if count % 10 == 0:
                producer.flush()
                print(f"\n  --- {count} purchases sent so far ---\n")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n  Interrupted. Flushing remaining messages...")
        producer.flush()
        print(f"  Total purchases produced: {count}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="ZMart synthetic data generator for Kafka Streams tutorial.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run: seed stores, then stream purchases at 1/sec
  python generate.py

  # Stream at 3 purchases/sec with 100 unique customers
  python generate.py --rate 3 --customers 100

  # Only seed stores (useful after topic reset)
  python generate.py --mode stores-only

  # Only stream purchases (stores already seeded)
  python generate.py --mode purchases-only --rate 0.5
        """
    )
    p.add_argument(
        "--bootstrap-servers", default="localhost:9092",
        help="Kafka broker address(es). Default: localhost:9092"
    )
    p.add_argument(
        "--rate", type=float, default=1.0,
        help="Purchase events per second. Default: 1.0"
    )
    p.add_argument(
        "--customers", type=int, default=20,
        help="Number of unique customer IDs in the pool. "
             "Lower = more repeat purchases per customer = more interesting "
             "rewards accumulation. Default: 20"
    )
    p.add_argument(
        "--mode", choices=["both", "stores-only", "purchases-only"],
        default="both",
        help="What to produce. Default: both (seed stores first, then stream purchases)"
    )
    return p.parse_args()


def main():
    args = parse_args()

    # ── Kafka producer config ─────────────────────────────────────────────────
    # acks=all + retries = durable, at-least-once delivery from the producer side
    # compression = lz4 reduces network bandwidth for JSON payloads
    producer_config = {
        "bootstrap.servers": args.bootstrap_servers,
        "acks":              "all",           # wait for all in-sync replicas
        "retries":           3,
        "linger.ms":         5,               # batch messages for up to 5ms
        "compression.type":  "lz4",
        "client.id":         "zmart-data-generator",
    }

    producer = Producer(producer_config)
    customers = build_customer_pool(args.customers)
    employees = build_employee_pool()

    print("\n  ZMart Data Generator")
    print(f"  Bootstrap servers : {args.bootstrap_servers}")
    print(f"  Mode              : {args.mode}")

    if args.mode in ("both", "stores-only"):
        seed_stores(producer)

    if args.mode in ("both", "purchases-only"):
        stream_purchases(producer, args.rate, customers, employees)


if __name__ == "__main__":
    main()
