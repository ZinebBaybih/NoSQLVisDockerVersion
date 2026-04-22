"""
seed.py - Populate all 4 NoSQL databases with sample data for NoSQL Vis demo.
Run from the repo root:  python seed.py
Requirements: docker-compose must be running (docker-compose up -d)
"""

import argparse
import copy
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
SMALL_DEMO_RECORD_COUNT = 180

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


ECOMMERCE_CATEGORIES = [
    {"id": "EC01", "name": "Electronics"},
    {"id": "EC02", "name": "Books"},
    {"id": "EC03", "name": "Home"},
    {"id": "EC04", "name": "Sports"},
    {"id": "EC05", "name": "Beauty"},
]

ORDER_STATUSES = ["created", "paid", "packed", "shipped", "delivered", "returned"]


def make_ecommerce_customer(i):
    first_name = FIRST_NAMES[(i * 2) % len(FIRST_NAMES)]
    last_name = LAST_NAMES[(i * 5) % len(LAST_NAMES)]
    return {
        "customer_id": "CUST{:04d}".format(i),
        "name": "{} {}".format(first_name, last_name),
        "email": "customer{:04d}@shop.example".format(i),
        "country": COUNTRIES[i % len(COUNTRIES)],
        "segment": random.choice(["new", "regular", "loyal", "wholesale"]),
        "joined_at": _random_datetime_within(900),
    }


def make_ecommerce_product(i):
    category = ECOMMERCE_CATEGORIES[i % len(ECOMMERCE_CATEGORIES)]
    return {
        "product_id": "EP{:04d}".format(i),
        "name": "{} {}".format(random.choice(PRODUCT_NAMES[category["name"]]), i),
        "category_id": category["id"],
        "category": category["name"],
        "price": round(random.uniform(5.0, 750.0), 2),
        "stock": random.randint(0, 250),
        "rating": round(random.uniform(2.5, 5.0), 1),
    }


def make_order(i, customer_id=None):
    item_count = random.randint(1, 4)
    items = []
    total = 0.0
    for j in range(item_count):
        product_id = "EP{:04d}".format((i + j * 7) % SMALL_DEMO_RECORD_COUNT)
        quantity = random.randint(1, 3)
        unit_price = round(random.uniform(5.0, 250.0), 2)
        total += quantity * unit_price
        items.append({
            "product_id": product_id,
            "quantity": quantity,
            "unit_price": unit_price,
        })
    return {
        "order_id": "ORD{:05d}".format(i),
        "customer_id": customer_id or "CUST{:04d}".format(i % SMALL_DEMO_RECORD_COUNT),
        "status": random.choice(ORDER_STATUSES),
        "created_at": _random_datetime_within(365),
        "total": round(total, 2),
        "items": items,
    }


CLINICS = [
    {"id": "CL01", "name": "Central Smile Clinic", "city": "Casablanca"},
    {"id": "CL02", "name": "North Dental Care", "city": "Rabat"},
    {"id": "CL03", "name": "Atlas Orthodontics", "city": "Marrakesh"},
]

DENTAL_SPECIALTIES = ["General Dentistry", "Orthodontics", "Endodontics", "Pediatric Dentistry"]

TREATMENTS = [
    {"id": "TR01", "name": "Cleaning", "category": "Preventive", "base_price": 60},
    {"id": "TR02", "name": "Filling", "category": "Restorative", "base_price": 120},
    {"id": "TR03", "name": "Root Canal", "category": "Endodontic", "base_price": 420},
    {"id": "TR04", "name": "Braces Check", "category": "Orthodontic", "base_price": 90},
    {"id": "TR05", "name": "Extraction", "category": "Surgical", "base_price": 180},
]


def make_patient(i):
    first_name = FIRST_NAMES[(i * 4) % len(FIRST_NAMES)]
    last_name = LAST_NAMES[(i * 6) % len(LAST_NAMES)]
    return {
        "patient_id": "PAT{:04d}".format(i),
        "name": "{} {}".format(first_name, last_name),
        "email": "patient{:04d}@clinic.example".format(i),
        "age": random.randint(8, 78),
        "insurance": random.choice(["basic", "premium", "student", "none"]),
        "registered_at": _random_datetime_within(1200),
    }


def make_dentist(i):
    first_name = FIRST_NAMES[(i * 3) % len(FIRST_NAMES)]
    last_name = LAST_NAMES[(i * 2) % len(LAST_NAMES)]
    clinic = CLINICS[i % len(CLINICS)]
    return {
        "dentist_id": "DEN{:03d}".format(i),
        "name": "Dr. {} {}".format(first_name, last_name),
        "specialty": DENTAL_SPECIALTIES[i % len(DENTAL_SPECIALTIES)],
        "clinic_id": clinic["id"],
        "years_experience": random.randint(2, 22),
    }


def make_appointment(i, patient_id=None):
    treatment = TREATMENTS[i % len(TREATMENTS)]
    return {
        "appointment_id": "APT{:05d}".format(i),
        "patient_id": patient_id or "PAT{:04d}".format(i % SMALL_DEMO_RECORD_COUNT),
        "dentist_id": "DEN{:03d}".format(i % 12),
        "treatment_id": treatment["id"],
        "clinic_id": CLINICS[i % len(CLINICS)]["id"],
        "date": _random_datetime_within(180),
        "status": random.choice(["scheduled", "completed", "cancelled", "follow-up"]),
        "cost": round(treatment["base_price"] * random.uniform(0.8, 1.4), 2),
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

    db.books.insert_many(copy.deepcopy(BOOKS))
    db.members.insert_many(copy.deepcopy(MEMBERS))
    db.loans.insert_many(copy.deepcopy(LOANS))

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
    edu.students.insert_many(copy.deepcopy(students))
    edu.courses.insert_many(copy.deepcopy(COURSES))
    edu.branches.insert_many(copy.deepcopy(BRANCHES))
    edu.enrollments.insert_many(copy.deepcopy(enrollments))

    print("   OK education_demo.students    : {} documents".format(edu.students.count_documents({})))
    print("   OK education_demo.courses     : {} documents".format(edu.courses.count_documents({})))
    print("   OK education_demo.branches    : {} documents".format(edu.branches.count_documents({})))
    print("   OK education_demo.enrollments : {} documents".format(edu.enrollments.count_documents({})))

    ecommerce = client["ecommerce_demo"]
    ecommerce.customers.drop()
    ecommerce.products.drop()
    ecommerce.categories.drop()
    ecommerce.orders.drop()
    ecommerce.carts.drop()

    customers = [make_ecommerce_customer(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    products = [make_ecommerce_product(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    orders = [
        make_order(i, customer_id=customers[i % len(customers)]["customer_id"])
        for i in range(SMALL_DEMO_RECORD_COUNT * 2)
    ]
    carts = [
        {
            "cart_id": "CART{:04d}".format(i),
            "customer_id": customers[i]["customer_id"],
            "items": [orders[i]["items"][0]],
            "updated_at": _random_datetime_within(30),
        }
        for i in range(min(80, len(customers)))
    ]
    ecommerce.customers.insert_many(copy.deepcopy(customers))
    ecommerce.products.insert_many(copy.deepcopy(products))
    ecommerce.categories.insert_many(copy.deepcopy(ECOMMERCE_CATEGORIES))
    ecommerce.orders.insert_many(copy.deepcopy(orders))
    ecommerce.carts.insert_many(copy.deepcopy(carts))

    print("   OK ecommerce_demo.customers  : {} documents".format(ecommerce.customers.count_documents({})))
    print("   OK ecommerce_demo.products   : {} documents".format(ecommerce.products.count_documents({})))
    print("   OK ecommerce_demo.categories : {} documents".format(ecommerce.categories.count_documents({})))
    print("   OK ecommerce_demo.orders     : {} documents".format(ecommerce.orders.count_documents({})))
    print("   OK ecommerce_demo.carts      : {} documents".format(ecommerce.carts.count_documents({})))

    dentist = client["dentist_demo"]
    dentist.patients.drop()
    dentist.dentists.drop()
    dentist.clinics.drop()
    dentist.treatments.drop()
    dentist.appointments.drop()

    patients = [make_patient(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    dentists = [make_dentist(i) for i in range(12)]
    appointments = [
        make_appointment(i, patient_id=patients[i % len(patients)]["patient_id"])
        for i in range(SMALL_DEMO_RECORD_COUNT * 2)
    ]
    dentist.patients.insert_many(copy.deepcopy(patients))
    dentist.dentists.insert_many(copy.deepcopy(dentists))
    dentist.clinics.insert_many(copy.deepcopy(CLINICS))
    dentist.treatments.insert_many(copy.deepcopy(TREATMENTS))
    dentist.appointments.insert_many(copy.deepcopy(appointments))

    print("   OK dentist_demo.patients     : {} documents".format(dentist.patients.count_documents({})))
    print("   OK dentist_demo.dentists     : {} documents".format(dentist.dentists.count_documents({})))
    print("   OK dentist_demo.clinics      : {} documents".format(dentist.clinics.count_documents({})))
    print("   OK dentist_demo.treatments   : {} documents".format(dentist.treatments.count_documents({})))
    print("   OK dentist_demo.appointments : {} documents".format(dentist.appointments.count_documents({})))
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
        pipe.hset("department:{}".format(department["id"]), mapping={
            "id": department["id"],
            "name": department["name"],
            "building": department["building"],
        })
        pipe.sadd("departments", department["id"])

    for branch in BRANCHES:
        pipe.hset("branch:{}".format(branch["id"]), mapping={
            "id": branch["id"],
            "name": branch["name"],
            "department_id": branch["department_id"],
        })
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

    ecommerce = redis.Redis(host="localhost", port=6379, db=3, decode_responses=True)
    ecommerce.flushdb()
    pipe = ecommerce.pipeline()
    ecommerce_customers = [make_ecommerce_customer(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    ecommerce_products = [make_ecommerce_product(i) for i in range(SMALL_DEMO_RECORD_COUNT)]

    pipe.set("store:name", "NoSQL Vis Shop")
    pipe.set("store:active_customers", str(len(ecommerce_customers)))
    for category in ECOMMERCE_CATEGORIES:
        pipe.hset("category:{}".format(category["id"]), mapping={
            "id": category["id"],
            "name": category["name"],
        })
        pipe.sadd("categories", category["id"])
    for product in ecommerce_products:
        pipe.hset("product:{}".format(product["product_id"]), mapping={
            "product_id": product["product_id"],
            "name": product["name"],
            "category_id": product["category_id"],
            "category": product["category"],
            "price": str(product["price"]),
            "stock": str(product["stock"]),
            "rating": str(product["rating"]),
        })
        pipe.zadd("products:by_rating", {product["product_id"]: product["rating"]})
        pipe.sadd("products:category:{}".format(product["category_id"]), product["product_id"])
    for i, customer in enumerate(ecommerce_customers):
        order = make_order(i, customer_id=customer["customer_id"])
        pipe.hset("customer:{}".format(customer["customer_id"]), mapping=customer)
        pipe.hset("order:{}".format(order["order_id"]), mapping={
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "status": order["status"],
            "created_at": order["created_at"],
            "total": str(order["total"]),
        })
        pipe.rpush("customer:{}:orders".format(customer["customer_id"]), order["order_id"])
        pipe.xadd("stream:orders", {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "status": order["status"],
            "total": str(order["total"]),
        }, maxlen=500, approximate=True)
        if (i + 1) % 100 == 0:
            pipe.execute()
    pipe.execute()
    print("   OK Redis DB 3 ecommerce mixed keys : {}".format(ecommerce.dbsize()))

    dentist = redis.Redis(host="localhost", port=6379, db=4, decode_responses=True)
    dentist.flushdb()
    pipe = dentist.pipeline()
    patients = [make_patient(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    dentists = [make_dentist(i) for i in range(12)]

    pipe.set("clinic:network", "NoSQL Vis Dental")
    pipe.set("clinic:patient_count", str(len(patients)))
    for clinic in CLINICS:
        pipe.hset("clinic:{}".format(clinic["id"]), mapping={
            "id": clinic["id"],
            "name": clinic["name"],
            "city": clinic["city"],
        })
        pipe.sadd("clinics", clinic["id"])
    for treatment in TREATMENTS:
        pipe.hset("treatment:{}".format(treatment["id"]), mapping={
            "id": treatment["id"],
            "name": treatment["name"],
            "category": treatment["category"],
            "base_price": str(treatment["base_price"]),
        })
        pipe.sadd("treatments:{}".format(treatment["category"]), treatment["id"])
    for dentist_row in dentists:
        pipe.hset("dentist:{}".format(dentist_row["dentist_id"]), mapping={
            "dentist_id": dentist_row["dentist_id"],
            "name": dentist_row["name"],
            "specialty": dentist_row["specialty"],
            "clinic_id": dentist_row["clinic_id"],
            "years_experience": str(dentist_row["years_experience"]),
        })
        pipe.sadd("dentists:specialty:{}".format(dentist_row["specialty"].replace(" ", "_")), dentist_row["dentist_id"])
    for i, patient in enumerate(patients):
        appointment = make_appointment(i, patient_id=patient["patient_id"])
        pipe.hset("patient:{}".format(patient["patient_id"]), mapping={
            "patient_id": patient["patient_id"],
            "name": patient["name"],
            "email": patient["email"],
            "age": str(patient["age"]),
            "insurance": patient["insurance"],
            "registered_at": patient["registered_at"],
        })
        pipe.hset("appointment:{}".format(appointment["appointment_id"]), mapping={
            "appointment_id": appointment["appointment_id"],
            "patient_id": appointment["patient_id"],
            "dentist_id": appointment["dentist_id"],
            "treatment_id": appointment["treatment_id"],
            "clinic_id": appointment["clinic_id"],
            "date": appointment["date"],
            "status": appointment["status"],
            "cost": str(appointment["cost"]),
        })
        pipe.rpush("patient:{}:appointments".format(patient["patient_id"]), appointment["appointment_id"])
        pipe.zadd("appointments:by_cost", {appointment["appointment_id"]: appointment["cost"]})
        pipe.xadd("stream:appointments", {
            "appointment_id": appointment["appointment_id"],
            "patient_id": appointment["patient_id"],
            "status": appointment["status"],
        }, maxlen=500, approximate=True)
        if (i + 1) % 100 == 0:
            pipe.execute()
    pipe.execute()
    print("   OK Redis DB 4 dentist mixed keys : {}".format(dentist.dbsize()))


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

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS ecommerce_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
    session.set_keyspace("ecommerce_demo")

    for table in ("orders", "carts", "products", "customers", "categories"):
        session.execute("DROP TABLE IF EXISTS {}".format(table))

    session.execute("""
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    country TEXT,
    segment TEXT,
    joined_at TEXT
)
""")
    session.execute("""
CREATE TABLE categories (
    category_id TEXT PRIMARY KEY,
    name TEXT
)
""")
    session.execute("""
CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    name TEXT,
    category_id TEXT,
    category TEXT,
    price FLOAT,
    stock INT,
    rating FLOAT
)
""")
    session.execute("""
CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    status TEXT,
    created_at TEXT,
    total FLOAT,
    item_count INT
)
""")
    session.execute("""
CREATE TABLE carts (
    cart_id TEXT PRIMARY KEY,
    customer_id TEXT,
    product_id TEXT,
    quantity INT,
    updated_at TEXT
)
""")

    customer_stmt = session.prepare(
        "INSERT INTO customers (customer_id, name, email, country, segment, joined_at) VALUES (?, ?, ?, ?, ?, ?)"
    )
    category_stmt = session.prepare(
        "INSERT INTO categories (category_id, name) VALUES (?, ?)"
    )
    product_stmt = session.prepare(
        "INSERT INTO products (product_id, name, category_id, category, price, stock, rating) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )
    order_stmt = session.prepare(
        "INSERT INTO orders (order_id, customer_id, status, created_at, total, item_count) VALUES (?, ?, ?, ?, ?, ?)"
    )
    cart_stmt = session.prepare(
        "INSERT INTO carts (cart_id, customer_id, product_id, quantity, updated_at) VALUES (?, ?, ?, ?, ?)"
    )

    ecommerce_customers = [make_ecommerce_customer(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    ecommerce_products = [make_ecommerce_product(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    for customer in ecommerce_customers:
        session.execute(customer_stmt, (
            customer["customer_id"], customer["name"], customer["email"],
            customer["country"], customer["segment"], customer["joined_at"]
        ))
    for category in ECOMMERCE_CATEGORIES:
        session.execute(category_stmt, (category["id"], category["name"]))
    for product in ecommerce_products:
        session.execute(product_stmt, (
            product["product_id"], product["name"], product["category_id"],
            product["category"], product["price"], product["stock"], product["rating"]
        ))
    for i in range(SMALL_DEMO_RECORD_COUNT * 2):
        order = make_order(i, customer_id=ecommerce_customers[i % len(ecommerce_customers)]["customer_id"])
        session.execute(order_stmt, (
            order["order_id"], order["customer_id"], order["status"],
            order["created_at"], order["total"], len(order["items"])
        ))
    for i in range(80):
        session.execute(cart_stmt, (
            "CART{:04d}".format(i),
            ecommerce_customers[i % len(ecommerce_customers)]["customer_id"],
            ecommerce_products[i % len(ecommerce_products)]["product_id"],
            random.randint(1, 4),
            _random_datetime_within(30),
        ))

    session.execute("""
CREATE KEYSPACE IF NOT EXISTS dentist_demo
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
    session.set_keyspace("dentist_demo")

    for table in ("appointments", "patients", "dentists", "treatments", "clinics"):
        session.execute("DROP TABLE IF EXISTS {}".format(table))

    session.execute("""
CREATE TABLE patients (
    patient_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INT,
    insurance TEXT,
    registered_at TEXT
)
""")
    session.execute("""
CREATE TABLE dentists (
    dentist_id TEXT PRIMARY KEY,
    name TEXT,
    specialty TEXT,
    clinic_id TEXT,
    years_experience INT
)
""")
    session.execute("""
CREATE TABLE clinics (
    clinic_id TEXT PRIMARY KEY,
    name TEXT,
    city TEXT
)
""")
    session.execute("""
CREATE TABLE treatments (
    treatment_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    base_price FLOAT
)
""")
    session.execute("""
CREATE TABLE appointments (
    appointment_id TEXT PRIMARY KEY,
    patient_id TEXT,
    dentist_id TEXT,
    treatment_id TEXT,
    clinic_id TEXT,
    date TEXT,
    status TEXT,
    cost FLOAT
)
""")

    patient_stmt = session.prepare(
        "INSERT INTO patients (patient_id, name, email, age, insurance, registered_at) VALUES (?, ?, ?, ?, ?, ?)"
    )
    dentist_stmt = session.prepare(
        "INSERT INTO dentists (dentist_id, name, specialty, clinic_id, years_experience) VALUES (?, ?, ?, ?, ?)"
    )
    clinic_stmt = session.prepare(
        "INSERT INTO clinics (clinic_id, name, city) VALUES (?, ?, ?)"
    )
    treatment_stmt = session.prepare(
        "INSERT INTO treatments (treatment_id, name, category, base_price) VALUES (?, ?, ?, ?)"
    )
    appointment_stmt = session.prepare(
        "INSERT INTO appointments (appointment_id, patient_id, dentist_id, treatment_id, clinic_id, date, status, cost) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    )

    patients = [make_patient(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
    dentists = [make_dentist(i) for i in range(12)]
    for patient in patients:
        session.execute(patient_stmt, (
            patient["patient_id"], patient["name"], patient["email"],
            patient["age"], patient["insurance"], patient["registered_at"]
        ))
    for dentist_row in dentists:
        session.execute(dentist_stmt, (
            dentist_row["dentist_id"], dentist_row["name"], dentist_row["specialty"],
            dentist_row["clinic_id"], dentist_row["years_experience"]
        ))
    for clinic in CLINICS:
        session.execute(clinic_stmt, (clinic["id"], clinic["name"], clinic["city"]))
    for treatment in TREATMENTS:
        session.execute(treatment_stmt, (
            treatment["id"], treatment["name"], treatment["category"], treatment["base_price"]
        ))
    for i in range(SMALL_DEMO_RECORD_COUNT * 2):
        appointment = make_appointment(i, patient_id=patients[i % len(patients)]["patient_id"])
        session.execute(appointment_stmt, (
            appointment["appointment_id"], appointment["patient_id"], appointment["dentist_id"],
            appointment["treatment_id"], appointment["clinic_id"], appointment["date"],
            appointment["status"], appointment["cost"]
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
    print("   OK ecommerce_demo.customers   : {} rows".format(SMALL_DEMO_RECORD_COUNT))
    print("   OK ecommerce_demo.products    : {} rows".format(SMALL_DEMO_RECORD_COUNT))
    print("   OK ecommerce_demo.orders      : {} rows".format(SMALL_DEMO_RECORD_COUNT * 2))
    print("   OK ecommerce_demo.carts       : {} rows".format(80))
    print("   OK dentist_demo.patients      : {} rows".format(SMALL_DEMO_RECORD_COUNT))
    print("   OK dentist_demo.dentists      : {} rows".format(12))
    print("   OK dentist_demo.appointments  : {} rows".format(SMALL_DEMO_RECORD_COUNT * 2))


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

        ecommerce_customers = [make_ecommerce_customer(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
        ecommerce_products = [make_ecommerce_product(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
        for category in ECOMMERCE_CATEGORIES:
            s.run(
                "CREATE (:Category {id:$id,name:$name})",
                id=category["id"], name=category["name"]
            )
        for customer in ecommerce_customers:
            s.run(
                """
CREATE (:Customer {
    id:$id,
    name:$name,
    email:$email,
    country:$country,
    segment:$segment,
    joined_at:$joined_at
})
""",
                id=customer["customer_id"], name=customer["name"], email=customer["email"],
                country=customer["country"], segment=customer["segment"], joined_at=customer["joined_at"]
            )
        for product in ecommerce_products:
            s.run(
                """
CREATE (:EcommerceProduct {
    id:$id,
    name:$name,
    category_id:$category_id,
    price:$price,
    stock:$stock,
    rating:$rating
})
""",
                id=product["product_id"], name=product["name"], category_id=product["category_id"],
                price=product["price"], stock=product["stock"], rating=product["rating"]
            )
            s.run(
                "MATCH (p:EcommerceProduct {id:$pid}),(c:Category {id:$cid}) "
                "CREATE (p)-[:IN_CATEGORY]->(c)",
                pid=product["product_id"], cid=product["category_id"]
            )
        for i in range(SMALL_DEMO_RECORD_COUNT):
            order = make_order(i, customer_id=ecommerce_customers[i]["customer_id"])
            s.run(
                "CREATE (:PurchaseOrder {id:$id,status:$status,created_at:$created_at,total:$total})",
                id=order["order_id"], status=order["status"],
                created_at=order["created_at"], total=order["total"]
            )
            s.run(
                "MATCH (c:Customer {id:$cid}),(o:PurchaseOrder {id:$oid}) "
                "CREATE (c)-[:PLACED]->(o)",
                cid=order["customer_id"], oid=order["order_id"]
            )
            for item in order["items"][:2]:
                s.run(
                    "MATCH (o:PurchaseOrder {id:$oid}),(p:EcommerceProduct {id:$pid}) "
                    "CREATE (o)-[:CONTAINS {quantity:$quantity,unit_price:$unit_price}]->(p)",
                    oid=order["order_id"], pid=item["product_id"],
                    quantity=item["quantity"], unit_price=item["unit_price"]
                )
            if i < 60:
                cart_id = "CART{:04d}".format(i)
                product_id = order["items"][0]["product_id"]
                s.run("CREATE (:Cart {id:$id,updated_at:$updated_at})", id=cart_id, updated_at=_random_datetime_within(30))
                s.run(
                    "MATCH (c:Customer {id:$cid}),(cart:Cart {id:$cart_id}),(p:EcommerceProduct {id:$pid}) "
                    "CREATE (c)-[:OWNS]->(cart) "
                    "CREATE (cart)-[:HOLDS]->(p)",
                    cid=order["customer_id"], cart_id=cart_id, pid=product_id
                )

        patients = [make_patient(i) for i in range(SMALL_DEMO_RECORD_COUNT)]
        dentists = [make_dentist(i) for i in range(12)]
        for clinic in CLINICS:
            s.run(
                "CREATE (:Clinic {id:$id,name:$name,city:$city})",
                id=clinic["id"], name=clinic["name"], city=clinic["city"]
            )
        for treatment in TREATMENTS:
            s.run(
                "CREATE (:Treatment {id:$id,name:$name,category:$category,base_price:$base_price})",
                id=treatment["id"], name=treatment["name"],
                category=treatment["category"], base_price=treatment["base_price"]
            )
        for dentist_row in dentists:
            s.run(
                """
CREATE (:Dentist {
    id:$id,
    name:$name,
    specialty:$specialty,
    clinic_id:$clinic_id,
    years_experience:$years_experience
})
""",
                id=dentist_row["dentist_id"], name=dentist_row["name"],
                specialty=dentist_row["specialty"], clinic_id=dentist_row["clinic_id"],
                years_experience=dentist_row["years_experience"]
            )
            s.run(
                "MATCH (d:Dentist {id:$did}),(c:Clinic {id:$cid}) "
                "CREATE (d)-[:WORKS_AT]->(c)",
                did=dentist_row["dentist_id"], cid=dentist_row["clinic_id"]
            )
        for patient in patients:
            s.run(
                """
CREATE (:Patient {
    id:$id,
    name:$name,
    email:$email,
    age:$age,
    insurance:$insurance,
    registered_at:$registered_at
})
""",
                id=patient["patient_id"], name=patient["name"], email=patient["email"],
                age=patient["age"], insurance=patient["insurance"], registered_at=patient["registered_at"]
            )
        for i in range(SMALL_DEMO_RECORD_COUNT):
            appointment = make_appointment(i, patient_id=patients[i]["patient_id"])
            s.run(
                """
CREATE (:Appointment {
    id:$id,
    date:$date,
    status:$status,
    cost:$cost
})
""",
                id=appointment["appointment_id"], date=appointment["date"],
                status=appointment["status"], cost=appointment["cost"]
            )
            s.run(
                "MATCH (p:Patient {id:$pid}),(a:Appointment {id:$aid}),"
                "(d:Dentist {id:$did}),(t:Treatment {id:$tid}),(c:Clinic {id:$cid}) "
                "CREATE (p)-[:BOOKED]->(a) "
                "CREATE (a)-[:WITH_DENTIST]->(d) "
                "CREATE (a)-[:FOR_TREATMENT]->(t) "
                "CREATE (a)-[:AT_CLINIC]->(c)",
                pid=appointment["patient_id"], aid=appointment["appointment_id"],
                did=appointment["dentist_id"], tid=appointment["treatment_id"],
                cid=appointment["clinic_id"]
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
        customers = s.run("MATCH (n:Customer) RETURN count(n) AS c").single()["c"]
        ecommerce_products_count = s.run("MATCH (n:EcommerceProduct) RETURN count(n) AS c").single()["c"]
        orders = s.run("MATCH (n:PurchaseOrder) RETURN count(n) AS c").single()["c"]
        patients = s.run("MATCH (n:Patient) RETURN count(n) AS c").single()["c"]
        dentists = s.run("MATCH (n:Dentist) RETURN count(n) AS c").single()["c"]
        appointments = s.run("MATCH (n:Appointment) RETURN count(n) AS c").single()["c"]
        rels    = s.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]

    print("   OK Book nodes    : {}".format(books))
    print("   OK Member nodes  : {}".format(members))
    print("   OK Genre nodes   : {}".format(genres))
    print("   OK Student nodes : {}".format(students))
    print("   OK Course nodes  : {}".format(courses))
    print("   OK Instructor nodes  : {}".format(instructors))
    print("   OK Department nodes  : {}".format(departments))
    print("   OK Branch nodes  : {}".format(branches))
    print("   OK Customer nodes : {}".format(customers))
    print("   OK EcommerceProduct nodes : {}".format(ecommerce_products_count))
    print("   OK Order nodes    : {}".format(orders))
    print("   OK Patient nodes  : {}".format(patients))
    print("   OK Dentist nodes  : {}".format(dentists))
    print("   OK Appointment nodes : {}".format(appointments))
    print("   OK Relationships : {} (library + education + ecommerce + dentist)".format(rels))
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
