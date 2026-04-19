"""
seed.py - Populate all 4 NoSQL databases with sample data for NoSQL Vis demo.
Run from the repo root:  python seed.py
Requirements: docker-compose must be running (docker-compose up -d)
"""

import time
import warnings
import traceback

warnings.filterwarnings("ignore")

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
# MAIN
# ==============================================================
if __name__ == "__main__":
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
    print("=" * 50)
