"""
seed.py - Populate all 4 NoSQL databases with sample data for NoSQL Vis demo.
Run from the repo root:  python seed.py
Requirements: docker-compose must be running (docker-compose up -d)
"""

import argparse
import random
import time
import uuid
import warnings
import traceback
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

# ==============================================================
# SYNTHETIC DATA GENERATORS
# ==============================================================
FIRST_NAMES = [
    "Alice", "Bob", "Carol", "David", "Eva", "Frank",
    "Grace", "Henry", "Ivy", "Jack", "Kara", "Liam",
    "Maya", "Noah", "Olivia", "Paul", "Quinn", "Ruby",
    "Sam", "Tina", "Uma", "Victor", "Wendy", "Yara",
]

LAST_NAMES = [
    "Martin", "Johnson", "White", "Brown", "Green",
    "Walker", "Young", "Hall", "Allen", "King",
    "Wright", "Scott", "Baker", "Carter", "Murphy",
]

COUNTRIES = [
    "United States", "Canada", "Mexico", "Brazil", "Argentina",
    "United Kingdom", "France", "Germany", "Spain", "Italy",
    "Morocco", "Egypt", "South Africa", "Nigeria", "India",
    "China", "Japan", "South Korea", "Australia", "New Zealand",
]

PRODUCT_CATEGORIES = [
    "Electronics", "Books", "Home", "Sports", "Beauty",
    "Fashion", "Toys", "Grocery", "Office", "Automotive",
]

PRODUCT_NAMES = {
    "Electronics": ["Wireless Earbuds", "4K Monitor", "Smartwatch", "Bluetooth Speaker", "Portable SSD", "Gaming Mouse"],
    "Books": ["Data Systems Guide", "Python Patterns", "Graph Thinking", "Cloud Basics", "AI Essentials", "Distributed Design"],
    "Home": ["Ceramic Vase", "LED Desk Lamp", "Memory Foam Pillow", "Air Purifier", "Wall Clock", "Storage Basket"],
    "Sports": ["Yoga Mat", "Running Belt", "Dumbbell Set", "Cycling Gloves", "Fitness Tracker", "Resistance Bands"],
    "Beauty": ["Hydrating Serum", "Daily Cleanser", "SPF Moisturizer", "Lip Balm Set", "Hair Repair Mask", "Body Scrub"],
    "Fashion": ["Classic Hoodie", "Denim Jacket", "Leather Wallet", "Canvas Sneakers", "Travel Backpack", "Wool Scarf"],
    "Toys": ["Puzzle Cube", "STEM Robot Kit", "Wooden Train", "Building Blocks", "RC Car", "Plush Panda"],
    "Grocery": ["Organic Coffee", "Sea Salt Crackers", "Dark Chocolate", "Olive Oil", "Herbal Tea", "Granola Mix"],
    "Office": ["Mechanical Keyboard", "Notebook Set", "Ergonomic Chair", "Desk Organizer", "Laser Pointer", "Whiteboard Kit"],
    "Automotive": ["Dash Camera", "Car Vacuum", "Phone Mount", "Seat Cushion", "Tire Inflator", "LED Headlight Kit"],
}

ACTION_TYPES = [
    "view", "click", "search", "purchase",
    "refund", "login", "logout", "share",
]

TAG_WORDS = [
    "new", "sale", "premium", "eco",
    "smart", "portable", "durable", "compact",
    "wireless", "popular", "classic", "gift",
]


def _random_datetime_within(days_back):
    now = datetime.utcnow()
    delta = timedelta(
        days=random.randint(0, days_back),
        seconds=random.randint(0, 86399),
    )
    return (now - delta).replace(microsecond=0).isoformat()


def make_user(i):
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    full_name = "{} {}".format(first_name, last_name)
    email_name = "{}.{}".format(first_name.lower(), last_name.lower())
    return {
        "user_id": "U{:06d}".format(i),
        "name": full_name,
        "email": "{}{}@example.com".format(email_name, i),
        "country": random.choice(COUNTRIES),
        "score": random.randint(0, 1000),
        "last_login": _random_datetime_within(730),
    }


def make_product(i):
    category = random.choice(PRODUCT_CATEGORIES)
    return {
        "product_id": "P{:06d}".format(i),
        "name": random.choice(PRODUCT_NAMES[category]),
        "category": category,
        "price": round(random.uniform(1.99, 999.99), 2),
        "stock": random.randint(0, 500),
        "tags": random.sample(TAG_WORDS, random.randint(2, 4)),
        "rating": round(random.uniform(1.0, 5.0), 1),
    }


def make_event(i):
    upper_bound = max(i, 1)
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": "U{:06d}".format(random.randint(0, upper_bound)),
        "action": random.choice(ACTION_TYPES),
        "timestamp": _random_datetime_within(365),
        "value": round(random.uniform(0, 500), 2),
    }


def _format_progress(current, total):
    return "   ↳ {:,} / {:,} inserted...".format(current, total)


def _run_neo4j_batch(session, query, rows):
    result = session.run(query, rows=rows)
    result.consume()

# ─────────────────────────────────────────────
# SAMPLE DATA
# ─────────────────────────────────────────────

BOOKS = [
    {"title": "The Great Gatsby",        "author": "F. Scott Fitzgerald", "year": 1925, "genre": "Fiction",     "copies": 3},
    {"title": "To Kill a Mockingbird",   "author": "Harper Lee",          "year": 1960, "genre": "Fiction",     "copies": 5},
    {"title": "1984",                    "author": "George Orwell",       "year": 1949, "genre": "Dystopia",    "copies": 4},
    {"title": "Brave New World",         "author": "Aldous Huxley",       "year": 1932, "genre": "Dystopia",    "copies": 2},
    {"title": "The Hobbit",              "author": "J.R.R. Tolkien",      "year": 1937, "genre": "Fantasy",     "copies": 6},
    {"title": "Dune",                    "author": "Frank Herbert",       "year": 1965, "genre": "Sci-Fi",      "copies": 3},
    {"title": "Foundation",              "author": "Isaac Asimov",        "year": 1951, "genre": "Sci-Fi",      "copies": 4},
    {"title": "The Alchemist",           "author": "Paulo Coelho",        "year": 1988, "genre": "Fiction",     "copies": 7},
    {"title": "Sapiens",                 "author": "Yuval Noah Harari",   "year": 2011, "genre": "Non-Fiction", "copies": 5},
    {"title": "Thinking Fast and Slow",  "author": "Daniel Kahneman",     "year": 2011, "genre": "Non-Fiction", "copies": 3},
]

MEMBERS = [
    {"id": "M001", "name": "Alice Martin", "email": "alice@example.com", "plan": "premium"},
    {"id": "M002", "name": "Bob Johnson",  "email": "bob@example.com",   "plan": "basic"},
    {"id": "M003", "name": "Carol White",  "email": "carol@example.com", "plan": "premium"},
    {"id": "M004", "name": "David Brown",  "email": "david@example.com", "plan": "basic"},
    {"id": "M005", "name": "Eva Green",    "email": "eva@example.com",   "plan": "student"},
]

LOANS = [
    {"loan_id": "L001", "member_id": "M001", "book_title": "1984",            "date": "2024-01-10", "returned": True},
    {"loan_id": "L002", "member_id": "M002", "book_title": "Dune",            "date": "2024-02-14", "returned": False},
    {"loan_id": "L003", "member_id": "M003", "book_title": "The Hobbit",      "date": "2024-03-05", "returned": True},
    {"loan_id": "L004", "member_id": "M001", "book_title": "Foundation",      "date": "2024-03-20", "returned": False},
    {"loan_id": "L005", "member_id": "M005", "book_title": "Sapiens",         "date": "2024-04-01", "returned": True},
    {"loan_id": "L006", "member_id": "M004", "book_title": "Brave New World", "date": "2024-04-15", "returned": False},
]


# ==============================================================
# 1. MONGODB
# ==============================================================
def seed_mongodb():
    print("\n[MongoDB] Seeding...")
    from pymongo import MongoClient
    client = MongoClient("mongodb://localhost:27017")
    db = client["library_demo"]

    db.books.drop()
    db.members.drop()
    db.loans.drop()

    db.books.insert_many(BOOKS)
    db.members.insert_many(MEMBERS)
    db.loans.insert_many(LOANS)

    print("   OK books   : {} documents".format(db.books.count_documents({})))
    print("   OK members : {} documents".format(db.members.count_documents({})))
    print("   OK loans   : {} documents".format(db.loans.count_documents({})))
    client.close()


# ==============================================================
# 2. REDIS
# ==============================================================
def seed_redis():
    print("\n[Redis] Seeding...")
    import redis

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    r.flushdb()

    r.set("library:total_books",   str(len(BOOKS)))
    r.set("library:total_members", str(len(MEMBERS)))
    r.set("library:open_loans",    str(sum(1 for l in LOANS if not l["returned"])))
    r.set("library:city",          "Geneva")

    for m in MEMBERS:
        r.hset("member:{}".format(m["id"]), mapping={
            "name": m["name"], "email": m["email"], "plan": m["plan"]
        })
    for i, b in enumerate(BOOKS, 1):
        r.hset("book:B{:03}".format(i), mapping={
            "title": b["title"], "author": b["author"],
            "year": str(b["year"]), "genre": b["genre"], "copies": str(b["copies"])
        })
    for b in BOOKS:
        r.sadd("genres", b["genre"])
    for b in BOOKS:
        r.zadd("books:by_year", {b["title"]: b["year"]})
    for l in LOANS:
        status = "returned" if l["returned"] else "on-loan"
        r.rpush("activity:log", "{} | {} | {} | {}".format(
            l["date"], l["member_id"], l["book_title"], status))

    print("   OK {} keys inserted".format(r.dbsize()))


# ==============================================================
# 3. CASSANDRA
# ==============================================================
def seed_cassandra():
    print("\n[Cassandra] Seeding...")
    from cassandra.cluster import Cluster

    cluster = None
    session = None

    print("   Waiting for Cassandra to be ready...")
    for attempt in range(12):
        try:
            cluster = Cluster(["localhost"], port=9042)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            print("   Cassandra ready on attempt {}".format(attempt + 1))
            break
        except Exception as e:
            print("   Waiting for Cassandra... ({}/12) - {}".format(attempt + 1, e))
            if session:
                try:
                    session.shutdown()
                except Exception:
                    pass
                session = None
            if cluster:
                try:
                    cluster.shutdown()
                except Exception:
                    pass
                cluster = None
            time.sleep(5)
    else:
        print("   ERROR: Cassandra not ready after 60s.")
        return

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS library_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
    session.set_keyspace("library_demo")

    session.execute("DROP TABLE IF EXISTS loans")
    session.execute("DROP TABLE IF EXISTS members")
    session.execute("DROP TABLE IF EXISTS books")

    session.execute("""
CREATE TABLE books (
    book_id TEXT PRIMARY KEY,
    title   TEXT,
    author  TEXT,
    year    INT,
    genre   TEXT,
    copies  INT
)
""")
    session.execute("""
CREATE TABLE members (
    member_id TEXT PRIMARY KEY,
    name      TEXT,
    email     TEXT,
    plan      TEXT
)
""")
    session.execute("""
CREATE TABLE loans (
    loan_id    TEXT PRIMARY KEY,
    member_id  TEXT,
    book_title TEXT,
    date       TEXT,
    returned   BOOLEAN
)
""")

    books_stmt = session.prepare(
        "INSERT INTO books (book_id, title, author, year, genre, copies) VALUES (?, ?, ?, ?, ?, ?)"
    )
    members_stmt = session.prepare(
        "INSERT INTO members (member_id, name, email, plan) VALUES (?, ?, ?, ?)"
    )
    loans_stmt = session.prepare(
        "INSERT INTO loans (loan_id, member_id, book_title, date, returned) VALUES (?, ?, ?, ?, ?)"
    )

    for i, b in enumerate(BOOKS, 1):
        session.execute(
            books_stmt,
            ("B{:03}".format(i), b["title"], b["author"], b["year"], b["genre"], b["copies"])
        )

    for m in MEMBERS:
        session.execute(
            members_stmt,
            (m["id"], m["name"], m["email"], m["plan"])
        )

    for l in LOANS:
        session.execute(
            loans_stmt,
            (l["loan_id"], l["member_id"], l["book_title"], l["date"], l["returned"])
        )

    session.shutdown()
    cluster.shutdown()

    print("   OK books   : {} rows".format(len(BOOKS)))
    print("   OK members : {} rows".format(len(MEMBERS)))
    print("   OK loans   : {} rows".format(len(LOANS)))


# ==============================================================
# 4. NEO4J
# ==============================================================
def seed_neo4j():
    print("\n[Neo4j] Seeding...")
    from neo4j import GraphDatabase

    driver = None
    for attempt in range(12):
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", "password")
            )
            with driver.session() as s:
                s.run("RETURN 1")
            print("   Connected on attempt {}".format(attempt + 1))
            break
        except Exception as e:
            print("   Waiting for Neo4j... ({}/12) - {}".format(attempt + 1, e))
            if driver:
                try:
                    driver.close()
                except Exception:
                    pass
            driver = None
            time.sleep(5)
    else:
        print("   ERROR: Could not connect to Neo4j after 60s.")
        return

    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")

        for i, b in enumerate(BOOKS, 1):
            s.run(
                "CREATE (:Book {id:$id,title:$title,author:$author,year:$year,genre:$genre,copies:$copies})",
                id="B{:03}".format(i), title=b["title"], author=b["author"],
                year=b["year"], genre=b["genre"], copies=b["copies"]
            )
        for m in MEMBERS:
            s.run(
                "CREATE (:Member {id:$id,name:$name,email:$email,plan:$plan})",
                id=m["id"], name=m["name"], email=m["email"], plan=m["plan"]
            )
        for g in set(b["genre"] for b in BOOKS):
            s.run("MERGE (:Genre {name:$name})", name=g)

        for i, b in enumerate(BOOKS, 1):
            s.run(
                "MATCH (b:Book {id:$bid}),(g:Genre {name:$genre}) CREATE (b)-[:BELONGS_TO]->(g)",
                bid="B{:03}".format(i), genre=b["genre"]
            )

        title_to_id = {"B{:03}".format(i+1): b["title"] for i, b in enumerate(BOOKS)}
        title_to_id = {v: k for k, v in title_to_id.items()}
        for l in LOANS:
            bid = title_to_id.get(l["book_title"])
            if bid:
                s.run(
                    "MATCH (m:Member {id:$mid}),(b:Book {id:$bid}) "
                    "CREATE (m)-[:BORROWED {loan_id:$lid,date:$date,returned:$ret}]->(b)",
                    mid=l["member_id"], bid=bid,
                    lid=l["loan_id"], date=l["date"], ret=l["returned"]
                )

    with driver.session() as s:
        books   = s.run("MATCH (n:Book)   RETURN count(n) AS c").single()["c"]
        members = s.run("MATCH (n:Member) RETURN count(n) AS c").single()["c"]
        genres  = s.run("MATCH (n:Genre)  RETURN count(n) AS c").single()["c"]
        rels    = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

    print("   OK Book nodes    : {}".format(books))
    print("   OK Member nodes  : {}".format(members))
    print("   OK Genre nodes   : {}".format(genres))
    print("   OK Relationships : {} (BORROWED + BELONGS_TO)".format(rels))
    driver.close()


# ==============================================================
# LARGE BENCHMARK SEEDERS
# ==============================================================
def seed_mongodb_large(size):
    print("\n📦 [MongoDB] Large benchmark seeding...")
    from pymongo import MongoClient

    client = MongoClient("mongodb://localhost:27017")
    db = client["benchmark"]

    db.users.drop()
    db.products.drop()

    batch_size = 1000
    users_inserted = 0
    products_inserted = 0

    for start in range(0, size, batch_size):
        end = min(start + batch_size, size)
        db.users.insert_many([make_user(i) for i in range(start, end)])
        users_inserted += end - start
        if users_inserted % 10000 == 0 or users_inserted == size:
            print(_format_progress(users_inserted, size))

    for start in range(0, size, batch_size):
        end = min(start + batch_size, size)
        db.products.insert_many([make_product(i) for i in range(start, end)])
        products_inserted += end - start
        if products_inserted % 10000 == 0 or products_inserted == size:
            print(_format_progress(products_inserted, size))

    user_count = db.users.count_documents({})
    product_count = db.products.count_documents({})
    print("   OK benchmark.users    : {:,} documents".format(user_count))
    print("   OK benchmark.products : {:,} documents".format(product_count))
    client.close()

    return {
        "database": "benchmark",
        "users": user_count,
        "products": product_count,
    }


def seed_redis_large(size):
    print("\n🔴 [Redis] Large benchmark seeding...")
    import redis

    r = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)
    r.flushdb()

    pipeline = r.pipeline()
    inserted = 0

    for i in range(size):
        user = make_user(i)
        pipeline.hset("user:{}".format(user["user_id"]), mapping={
            "user_id": user["user_id"],
            "name": user["name"],
            "email": user["email"],
            "country": user["country"],
            "score": str(user["score"]),
            "last_login": user["last_login"],
        })
        inserted += 1

        if inserted % 500 == 0:
            pipeline.execute()
        if inserted % 10000 == 0 or inserted == size:
            print(_format_progress(inserted, size))

    if inserted % 500 != 0:
        pipeline.execute()

    key_count = r.dbsize()
    print("   OK Redis DB 1 user:* hashes : {:,} keys".format(key_count))

    return {
        "db_index": 1,
        "user_hashes": key_count,
    }


def seed_cassandra_large(size):
    print("\n🟡 [Cassandra] Large benchmark seeding...")
    from cassandra.cluster import Cluster
    from cassandra.query import BatchStatement

    cluster = None
    session = None

    print("   Waiting for Cassandra to be ready...")
    for attempt in range(12):
        try:
            cluster = Cluster(["localhost"], port=9042)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            print("   Cassandra ready on attempt {}".format(attempt + 1))
            break
        except Exception as e:
            print("   Waiting for Cassandra... ({}/12) - {}".format(attempt + 1, e))
            if session:
                try:
                    session.shutdown()
                except Exception:
                    pass
                session = None
            if cluster:
                try:
                    cluster.shutdown()
                except Exception:
                    pass
                cluster = None
            time.sleep(5)
    else:
        raise RuntimeError("Cassandra not ready after 60s.")

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS benchmark
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
    session.set_keyspace("benchmark")

    session.execute("DROP TABLE IF EXISTS events")
    session.execute("DROP TABLE IF EXISTS users")

    session.execute("""
CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    user_id TEXT,
    action TEXT,
    timestamp TEXT,
    value FLOAT
)
""")
    session.execute("""
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    score INT,
    last_login TEXT
)
""")

    users_stmt = session.prepare(
        "INSERT INTO users (user_id, name, email, country, score, last_login) VALUES (?, ?, ?, ?, ?, ?)"
    )
    events_stmt = session.prepare(
        "INSERT INTO events (event_id, user_id, action, timestamp, value) VALUES (?, ?, ?, ?, ?)"
    )

    batch_size = 100
    users_inserted = 0
    events_inserted = 0

    for start in range(0, size, batch_size):
        end = min(start + batch_size, size)
        batch = BatchStatement()
        for i in range(start, end):
            user = make_user(i)
            batch.add(users_stmt, (
                user["user_id"], user["name"], user["email"], user["country"],
                user["score"], user["last_login"]
            ))
        session.execute(batch)
        users_inserted += end - start
        if users_inserted % 5000 == 0 or users_inserted == size:
            print(_format_progress(users_inserted, size))

    for start in range(0, size, batch_size):
        end = min(start + batch_size, size)
        batch = BatchStatement()
        for i in range(start, end):
            event = make_event(i)
            batch.add(events_stmt, (
                event["event_id"], event["user_id"], event["action"],
                event["timestamp"], event["value"]
            ))
        session.execute(batch)
        events_inserted += end - start
        if events_inserted % 5000 == 0 or events_inserted == size:
            print(_format_progress(events_inserted, size))

    session.shutdown()
    cluster.shutdown()

    print("   OK benchmark.events : {:,} rows".format(events_inserted))
    print("   OK benchmark.users  : {:,} rows".format(users_inserted))

    return {
        "keyspace": "benchmark",
        "users": users_inserted,
        "events": events_inserted,
    }


def seed_neo4j_large(size):
    print("\n🟢 [Neo4j] Large benchmark seeding...")
    from neo4j import GraphDatabase

    driver = None
    for attempt in range(12):
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", "password")
            )
            with driver.session() as s:
                s.run("RETURN 1")
            print("   Connected on attempt {}".format(attempt + 1))
            break
        except Exception as e:
            print("   Waiting for Neo4j... ({}/12) - {}".format(attempt + 1, e))
            if driver:
                try:
                    driver.close()
                except Exception:
                    pass
            driver = None
            time.sleep(5)
    else:
        raise RuntimeError("Could not connect to Neo4j after 60s.")

    batch_size = 500
    users_inserted = 0
    products_inserted = 0

    with driver.session() as s:
        s.run(
            "CREATE CONSTRAINT benchmark_user_id IF NOT EXISTS "
            "FOR (u:User) REQUIRE u.user_id IS UNIQUE"
        ).consume()
        s.run(
            "CREATE CONSTRAINT benchmark_product_id IF NOT EXISTS "
            "FOR (p:Product) REQUIRE p.product_id IS UNIQUE"
        ).consume()

        for start in range(0, size, batch_size):
            end = min(start + batch_size, size)
            rows = [make_user(i) for i in range(start, end)]
            _run_neo4j_batch(s, """
UNWIND $rows AS row
MERGE (u:User {user_id: row.user_id})
SET u.name = row.name,
    u.email = row.email,
    u.country = row.country,
    u.score = row.score,
    u.last_login = row.last_login
""", rows=rows)
            users_inserted += end - start
            if users_inserted % 2000 == 0 or users_inserted == size:
                print(_format_progress(users_inserted, size))

        for start in range(0, size, batch_size):
            end = min(start + batch_size, size)
            rows = [make_product(i) for i in range(start, end)]
            _run_neo4j_batch(s, """
UNWIND $rows AS row
MERGE (p:Product {product_id: row.product_id})
SET p.name = row.name,
    p.category = row.category,
    p.price = row.price,
    p.stock = row.stock,
    p.tags = row.tags,
    p.rating = row.rating
""", rows=rows)
            products_inserted += end - start
            if products_inserted % 2000 == 0 or products_inserted == size:
                print(_format_progress(products_inserted, size))

        follows_total = 0
        likes_total = 0
        follows_rows = []
        likes_rows = []

        for i in range(size):
            user_id = "U{:06d}".format(i)
            if size > 1:
                target_count = min(random.randint(2, 5), size - 1)
                follow_targets = set()
                while len(follow_targets) < target_count:
                    target = random.randrange(size)
                    if target != i:
                        follow_targets.add(target)
                for target in follow_targets:
                    follows_rows.append({
                        "user_id": user_id,
                        "target_user_id": "U{:06d}".format(target),
                    })
            for target in random.sample(range(size), min(random.randint(3, 8), size)):
                likes_rows.append({
                    "user_id": user_id,
                    "product_id": "P{:06d}".format(target),
                })

            if len(follows_rows) >= batch_size:
                _run_neo4j_batch(s, """
UNWIND $rows AS row
MATCH (u:User {user_id: row.user_id})
MATCH (v:User {user_id: row.target_user_id})
MERGE (u)-[:FOLLOWS]->(v)
""", rows=follows_rows)
                follows_total += len(follows_rows)
                follows_rows = []

            if len(likes_rows) >= batch_size:
                _run_neo4j_batch(s, """
UNWIND $rows AS row
MATCH (u:User {user_id: row.user_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (u)-[:LIKES]->(p)
""", rows=likes_rows)
                likes_total += len(likes_rows)
                likes_rows = []

            if (i + 1) % 2000 == 0 or i + 1 == size:
                print(_format_progress(i + 1, size))

        if follows_rows:
            _run_neo4j_batch(s, """
UNWIND $rows AS row
MATCH (u:User {user_id: row.user_id})
MATCH (v:User {user_id: row.target_user_id})
MERGE (u)-[:FOLLOWS]->(v)
""", rows=follows_rows)
            follows_total += len(follows_rows)

        if likes_rows:
            _run_neo4j_batch(s, """
UNWIND $rows AS row
MATCH (u:User {user_id: row.user_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (u)-[:LIKES]->(p)
""", rows=likes_rows)
            likes_total += len(likes_rows)

        user_count = s.run("MATCH (u:User) RETURN count(u) AS c").single()["c"]
        product_count = s.run("MATCH (p:Product) RETURN count(p) AS c").single()["c"]
        follows_count = s.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS c").single()["c"]
        likes_count = s.run("MATCH ()-[r:LIKES]->() RETURN count(r) AS c").single()["c"]

    print("   OK User nodes    : {:,}".format(user_count))
    print("   OK Product nodes : {:,}".format(product_count))
    print("   OK FOLLOWS rels  : {:,}".format(follows_count))
    print("   OK LIKES rels    : {:,}".format(likes_count))
    driver.close()

    return {
        "users": user_count,
        "products": product_count,
        "follows": follows_count,
        "likes": likes_count,
    }


# ==============================================================
# MAIN
# ==============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed NoSQL Vis demo and benchmark datasets.")
    parser.add_argument(
        "--large",
        action="store_true",
        help="Seed additional large benchmark datasets after the basic demo seed.",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=10000,
        help="Number of records to insert per database for the benchmark seed (default: 10000).",
    )
    args = parser.parse_args()
    if args.size <= 0:
        parser.error("--size must be a positive integer")

    print("=" * 50)
    print("  NoSQL Vis -- Database Seeder")
    print("=" * 50)
    print("Make sure docker-compose is running first.\n")

    errors = []

    for name, fn in [
        ("MongoDB",   seed_mongodb),
        ("Redis",     seed_redis),
        ("Cassandra", seed_cassandra),
        ("Neo4j",     seed_neo4j),
    ]:
        try:
            fn()
        except Exception:
            print("\n   ERROR in {}:".format(name))
            traceback.print_exc()
            errors.append(name)

    print("\n" + "=" * 50)
    if errors:
        print("  WARNING: Errors in: {}".format(", ".join(errors)))
        print("  Check that all containers are running: docker-compose ps")
    else:
        print("  All 4 databases seeded successfully!")
        print("  You can now launch: python app/main.py")

    if args.large:
        print("\n" + "=" * 50)
        print("  Large Benchmark Seed")
        print("  Target size per database: {:,}".format(args.size))
        print("=" * 50)

        large_results = {}
        large_errors = []

        for name, fn in [
            ("MongoDB", lambda: seed_mongodb_large(args.size)),
            ("Redis", lambda: seed_redis_large(args.size)),
            ("Cassandra", lambda: seed_cassandra_large(args.size)),
            ("Neo4j", lambda: seed_neo4j_large(args.size)),
        ]:
            try:
                large_results[name] = fn()
            except Exception:
                print("\n   ERROR in {} large seeder:".format(name))
                traceback.print_exc()
                large_errors.append(name)

        print("\n" + "=" * 50)
        print("  Large Seed Summary")
        print("=" * 50)
        if "MongoDB" in large_results:
            result = large_results["MongoDB"]
            print("  MongoDB   -> benchmark.users: {:,}, benchmark.products: {:,}".format(
                result["users"], result["products"]
            ))
        if "Redis" in large_results:
            result = large_results["Redis"]
            print("  Redis     -> DB {} user:* hashes: {:,}".format(
                result["db_index"], result["user_hashes"]
            ))
        if "Cassandra" in large_results:
            result = large_results["Cassandra"]
            print("  Cassandra -> benchmark.users: {:,}, benchmark.events: {:,}".format(
                result["users"], result["events"]
            ))
        if "Neo4j" in large_results:
            result = large_results["Neo4j"]
            print("  Neo4j     -> User nodes: {:,}, Product nodes: {:,}, FOLLOWS: {:,}, LIKES: {:,}".format(
                result["users"], result["products"], result["follows"], result["likes"]
            ))
        if large_errors:
            print("  WARNING: Large seed errors in: {}".format(", ".join(large_errors)))
        else:
            print("  Large benchmark datasets seeded successfully.")
    print("=" * 50)
