"""
seed.py - Populate all 4 NoSQL databases with sample data for NoSQL Vis demo.
Run from the repo root:  python seed.py
Requirements: docker-compose must be running (docker-compose up -d)
"""

import argparse
import random
import sys
import time
import uuid
import warnings
import traceback
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")

APP_DIR = Path(__file__).resolve().parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from config import (
    CASSANDRA_RESET_KEYSPACES,
    MONGODB_RESET_DATABASES,
    NEO4J_RESET_LABELS,
    REDIS_RESET_DATABASES,
)

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


def _clear_neo4j_benchmark_data(session):
    print("   Clearing existing Neo4j benchmark data...")
    batch_size = 5000

    for label in ("User", "Product"):
        deleted_total = 0
        while True:
            deleted = session.run(
                """
MATCH (n:`%s`)
WITH n LIMIT $limit
DETACH DELETE n
RETURN count(*) AS deleted
""" % label,
                limit=batch_size,
            ).single()["deleted"]
            deleted_total += deleted
            if deleted == 0:
                break

        if deleted_total:
            print("   -> removed {:,} {} nodes".format(deleted_total, label))


def reset_mongodb():
    from pymongo import MongoClient

    client = MongoClient("mongodb://localhost:27017")
    for db_name in MONGODB_RESET_DATABASES:
        client.drop_database(db_name)
    client.close()


def reset_redis():
    import redis

    for db_index in REDIS_RESET_DATABASES:
        r = redis.Redis(host="localhost", port=6379, db=db_index, decode_responses=True)
        r.flushdb()


def reset_cassandra():
    from cassandra.cluster import Cluster

    cluster = None
    session = None
    last_error = None
    for _ in range(12):
        try:
            cluster = Cluster(["localhost"], port=9042)
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            break
        except Exception as e:
            last_error = e
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
        raise RuntimeError("Could not reset Cassandra after 60s: {}".format(last_error))

    for keyspace in CASSANDRA_RESET_KEYSPACES:
        session.execute("DROP KEYSPACE IF EXISTS {}".format(keyspace))
    session.shutdown()
    cluster.shutdown()


def reset_neo4j():
    from neo4j import GraphDatabase

    driver = None
    last_error = None
    for _ in range(12):
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", "password")
            )
            with driver.session() as s:
                s.run("RETURN 1")
            break
        except Exception as e:
            last_error = e
            if driver:
                try:
                    driver.close()
                except Exception:
                    pass
                driver = None
            time.sleep(5)
    else:
        raise RuntimeError("Could not reset Neo4j after 60s: {}".format(last_error))

    with driver.session() as s:
        for label in NEO4J_RESET_LABELS:
            s.run("MATCH (n:`{}`) DETACH DELETE n".format(label)).consume()
    driver.close()

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

DEMO_RECORD_COUNT = 240

DEPARTMENTS = [
    {"id": "D01", "name": "Computer Science", "building": "Ada"},
    {"id": "D02", "name": "Data Science", "building": "Turing"},
    {"id": "D03", "name": "Information Systems", "building": "Lovelace"},
]

BRANCHES = [
    {"id": "B01", "name": "Master in Data Engineering", "department_id": "D02"},
    {"id": "B02", "name": "Software Engineering", "department_id": "D01"},
    {"id": "B03", "name": "Information Systems", "department_id": "D03"},
    {"id": "B04", "name": "Data Analytics", "department_id": "D02"},
]

COURSES = [
    {"id": "C101", "title": "Introduction to Databases", "level": "beginner", "department_id": "D01", "credits": 4},
    {"id": "C102", "title": "Document Databases", "level": "beginner", "department_id": "D02", "credits": 3},
    {"id": "C201", "title": "Key Value Stores", "level": "intermediate", "department_id": "D03", "credits": 3},
    {"id": "C202", "title": "Column Family Modeling", "level": "intermediate", "department_id": "D02", "credits": 4},
    {"id": "C301", "title": "Graph Data Modeling", "level": "advanced", "department_id": "D01", "credits": 4},
    {"id": "C302", "title": "NoSQL Visualization Lab", "level": "advanced", "department_id": "D03", "credits": 2},
]

INSTRUCTORS = [
    {"id": "I01", "name": "Nadia Karim", "department_id": "D01"},
    {"id": "I02", "name": "Omar Haddad", "department_id": "D02"},
    {"id": "I03", "name": "Leila Mansouri", "department_id": "D03"},
]


def make_student(i):
    first_name = FIRST_NAMES[i % len(FIRST_NAMES)]
    last_name = LAST_NAMES[(i * 3) % len(LAST_NAMES)]
    branch = BRANCHES[i % len(BRANCHES)]
    return {
        "student_id": "S{:04d}".format(i),
        "name": "{} {}".format(first_name, last_name),
        "email": "student{:04d}@university.example".format(i),
        "program": branch["name"],
        "branch_id": branch["id"],
        "year": random.randint(1, 5),
        "country": random.choice(COUNTRIES),
        "gpa": round(random.uniform(2.0, 4.0), 2),
    }


def make_enrollment(i, student_id=None):
    course = COURSES[i % len(COURSES)]
    return {
        "enrollment_id": "E{:05d}".format(i),
        "student_id": student_id or "S{:04d}".format(i % DEMO_RECORD_COUNT),
        "course_id": course["id"],
        "semester": random.choice(["2024-Fall", "2025-Spring", "2025-Fall", "2026-Spring"]),
        "status": random.choice(["active", "completed", "withdrawn"]),
        "grade": random.choice(["A", "B", "C", "In progress"]),
    }


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

    edu = client["education_demo"]
    edu.students.drop()
    edu.courses.drop()
    edu.enrollments.drop()
    edu.branches.drop()

    students = [make_student(i) for i in range(DEMO_RECORD_COUNT)]
    enrollments = [
        make_enrollment(i, student_id=students[i % len(students)]["student_id"])
        for i in range(DEMO_RECORD_COUNT * 2)
    ]
    edu.students.insert_many(students)
    edu.courses.insert_many(COURSES)
    edu.branches.insert_many(BRANCHES)
    edu.enrollments.insert_many(enrollments)

    print("   OK education_demo.students    : {} documents".format(edu.students.count_documents({})))
    print("   OK education_demo.courses     : {} documents".format(edu.courses.count_documents({})))
    print("   OK education_demo.branches    : {} documents".format(edu.branches.count_documents({})))
    print("   OK education_demo.enrollments : {} documents".format(edu.enrollments.count_documents({})))
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

    edu = redis.Redis(host="localhost", port=6379, db=2, decode_responses=True)
    edu.flushdb()
    pipe = edu.pipeline()
    students = [make_student(i) for i in range(DEMO_RECORD_COUNT)]

    pipe.set("campus:name", "NoSQL Vis University")
    pipe.set("campus:semester", "2026-Spring")
    pipe.set("campus:student_count", str(len(students)))

    for department in DEPARTMENTS:
        pipe.hset("department:{}".format(department["id"]), mapping=department)
        pipe.sadd("departments", department["id"])

    for branch in BRANCHES:
        pipe.hset("branch:{}".format(branch["id"]), mapping=branch)
        pipe.sadd("branches:{}".format(branch["department_id"]), branch["id"])

    for course in COURSES:
        pipe.hset("course:{}".format(course["id"]), mapping={
            "id": course["id"],
            "title": course["title"],
            "level": course["level"],
            "department_id": course["department_id"],
            "credits": str(course["credits"]),
        })
        pipe.sadd("courses:{}".format(course["level"]), course["id"])

    for i, student in enumerate(students):
        pipe.hset("student:{}".format(student["student_id"]), mapping={
            "student_id": student["student_id"],
            "name": student["name"],
            "email": student["email"],
            "program": student["program"],
            "branch_id": student["branch_id"],
            "year": str(student["year"]),
            "country": student["country"],
            "gpa": str(student["gpa"]),
        })
        pipe.zadd("students:by_gpa", {student["student_id"]: student["gpa"]})
        pipe.sadd("students:program:{}".format(student["program"].replace(" ", "_")), student["student_id"])
        pipe.rpush("activity:recent", "{} enrolled in {}".format(
            student["student_id"], COURSES[i % len(COURSES)]["id"]
        ))
        pipe.xadd("stream:enrollments", {
            "student_id": student["student_id"],
            "course_id": COURSES[i % len(COURSES)]["id"],
            "status": "active",
        }, maxlen=500, approximate=True)

        if (i + 1) % 100 == 0:
            pipe.execute()

    pipe.execute()
    print("   OK Redis DB 2 education mixed keys : {}".format(edu.dbsize()))


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

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS education_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
    session.set_keyspace("education_demo")

    session.execute("DROP TABLE IF EXISTS enrollments")
    session.execute("DROP TABLE IF EXISTS courses")
    session.execute("DROP TABLE IF EXISTS students")
    session.execute("DROP TABLE IF EXISTS branches")

    session.execute("""
CREATE TABLE students (
    student_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    program TEXT,
    branch_id TEXT,
    year INT,
    country TEXT,
    gpa FLOAT
)
""")
    session.execute("""
CREATE TABLE branches (
    branch_id TEXT PRIMARY KEY,
    name TEXT,
    department_id TEXT
)
""")
    session.execute("""
CREATE TABLE courses (
    course_id TEXT PRIMARY KEY,
    title TEXT,
    level TEXT,
    department_id TEXT,
    credits INT
)
""")
    session.execute("""
CREATE TABLE enrollments (
    enrollment_id TEXT PRIMARY KEY,
    student_id TEXT,
    course_id TEXT,
    semester TEXT,
    status TEXT,
    grade TEXT
)
""")

    students_stmt = session.prepare(
        "INSERT INTO students (student_id, name, email, program, branch_id, year, country, gpa) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )
    branches_stmt = session.prepare(
        "INSERT INTO branches (branch_id, name, department_id) VALUES (?, ?, ?)"
    )
    courses_stmt = session.prepare(
        "INSERT INTO courses (course_id, title, level, department_id, credits) VALUES (?, ?, ?, ?, ?)"
    )
    enrollments_stmt = session.prepare(
        "INSERT INTO enrollments (enrollment_id, student_id, course_id, semester, status, grade) VALUES (?, ?, ?, ?, ?, ?)"
    )

    students = [make_student(i) for i in range(DEMO_RECORD_COUNT)]
    for student in students:
        session.execute(students_stmt, (
            student["student_id"], student["name"], student["email"], student["program"],
            student["branch_id"], student["year"], student["country"], student["gpa"]
        ))
    for branch in BRANCHES:
        session.execute(branches_stmt, (
            branch["id"], branch["name"], branch["department_id"]
        ))
    for course in COURSES:
        session.execute(courses_stmt, (
            course["id"], course["title"], course["level"], course["department_id"], course["credits"]
        ))
    for i in range(DEMO_RECORD_COUNT * 2):
        enrollment = make_enrollment(i, student_id=students[i % len(students)]["student_id"])
        session.execute(enrollments_stmt, (
            enrollment["enrollment_id"], enrollment["student_id"], enrollment["course_id"],
            enrollment["semester"], enrollment["status"], enrollment["grade"]
        ))

    session.shutdown()
    cluster.shutdown()

    print("   OK books   : {} rows".format(len(BOOKS)))
    print("   OK members : {} rows".format(len(MEMBERS)))
    print("   OK loans   : {} rows".format(len(LOANS)))
    print("   OK education_demo.students    : {} rows".format(DEMO_RECORD_COUNT))
    print("   OK education_demo.courses     : {} rows".format(len(COURSES)))
    print("   OK education_demo.branches    : {} rows".format(len(BRANCHES)))
    print("   OK education_demo.enrollments : {} rows".format(DEMO_RECORD_COUNT * 2))


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

        for department in DEPARTMENTS:
            s.run(
                "CREATE (:Department {id:$id,name:$name,building:$building})",
                id=department["id"], name=department["name"], building=department["building"]
            )
        for branch in BRANCHES:
            s.run(
                "CREATE (:Branch {id:$id,name:$name,department_id:$department_id})",
                id=branch["id"], name=branch["name"], department_id=branch["department_id"]
            )
        for instructor in INSTRUCTORS:
            s.run(
                "CREATE (:Instructor {id:$id,name:$name,department_id:$department_id})",
                id=instructor["id"], name=instructor["name"], department_id=instructor["department_id"]
            )
        for course in COURSES:
            s.run(
                "CREATE (:Course {id:$id,title:$title,level:$level,department_id:$department_id,credits:$credits})",
                id=course["id"], title=course["title"], level=course["level"],
                department_id=course["department_id"], credits=course["credits"]
            )

        students = [make_student(i) for i in range(DEMO_RECORD_COUNT)]
        for student in students:
            s.run(
                """
CREATE (:Student {
    id:$id,
    name:$name,
    email:$email,
    program:$program,
    year:$year,
    country:$country,
    gpa:$gpa
})
""",
                id=student["student_id"], name=student["name"], email=student["email"],
                program=student["program"], year=student["year"],
                country=student["country"], gpa=student["gpa"]
            )

        for course in COURSES:
            s.run(
                "MATCH (c:Course {id:$cid}),(d:Department {id:$did}) "
                "CREATE (c)-[:PART_OF]->(d)",
                cid=course["id"], did=course["department_id"]
            )
        for branch in BRANCHES:
            s.run(
                "MATCH (b:Branch {id:$bid}),(d:Department {id:$did}) "
                "CREATE (b)-[:OFFERED_BY]->(d)",
                bid=branch["id"], did=branch["department_id"]
            )
        for i, instructor in enumerate(INSTRUCTORS):
            course_ids = [course["id"] for course in COURSES if course["department_id"] == instructor["department_id"]]
            for course_id in course_ids:
                s.run(
                    "MATCH (i:Instructor {id:$iid}),(c:Course {id:$cid}) "
                    "CREATE (i)-[:TEACHES]->(c)",
                    iid=instructor["id"], cid=course_id
                )
        for i, student in enumerate(students):
            first_course = COURSES[i % len(COURSES)]["id"]
            second_course = COURSES[(i + 2) % len(COURSES)]["id"]
            s.run(
                "MATCH (s:Student {id:$sid}),(b:Branch {id:$bid}) "
                "CREATE (s)-[:SPECIALIZES_IN]->(b)",
                sid=student["student_id"], bid=student["branch_id"]
            )
            for course_id in {first_course, second_course}:
                s.run(
                    "MATCH (s:Student {id:$sid}),(c:Course {id:$cid}) "
                    "CREATE (s)-[:ENROLLED_IN {semester:$semester}]->(c)",
                    sid=student["student_id"], cid=course_id,
                    semester=random.choice(["2025-Fall", "2026-Spring"])
                )

    with driver.session() as s:
        books   = s.run("MATCH (n:Book)   RETURN count(n) AS c").single()["c"]
        members = s.run("MATCH (n:Member) RETURN count(n) AS c").single()["c"]
        genres  = s.run("MATCH (n:Genre)  RETURN count(n) AS c").single()["c"]
        students = s.run("MATCH (n:Student) RETURN count(n) AS c").single()["c"]
        courses = s.run("MATCH (n:Course) RETURN count(n) AS c").single()["c"]
        instructors = s.run("MATCH (n:Instructor) RETURN count(n) AS c").single()["c"]
        departments = s.run("MATCH (n:Department) RETURN count(n) AS c").single()["c"]
        branches = s.run("MATCH (n:Branch) RETURN count(n) AS c").single()["c"]
        rels    = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

    print("   OK Book nodes    : {}".format(books))
    print("   OK Member nodes  : {}".format(members))
    print("   OK Genre nodes   : {}".format(genres))
    print("   OK Student nodes : {}".format(students))
    print("   OK Course nodes  : {}".format(courses))
    print("   OK Instructor nodes  : {}".format(instructors))
    print("   OK Department nodes  : {}".format(departments))
    print("   OK Branch nodes  : {}".format(branches))
    print("   OK Relationships : {} (library + education)".format(rels))
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

        _clear_neo4j_benchmark_data(s)

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

    for name, reset_fn, fn in [
        ("MongoDB",   reset_mongodb,   seed_mongodb),
        ("Redis",     reset_redis,     seed_redis),
        ("Cassandra", reset_cassandra, seed_cassandra),
        ("Neo4j",     reset_neo4j,     seed_neo4j),
    ]:
        try:
            print("\n[{}] Factory reset...".format(name))
            reset_fn()
            print("   OK reset complete")
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
