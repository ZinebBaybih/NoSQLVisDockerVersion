PREVIEW_LIMIT = 50
PAGE_SIZES = [50, 100, 500, 1000]
NEO4J_GRAPH_PREVIEW_LIMIT = 100

# Data scopes owned by NoSQL Vis and safe for seed.py to reset.
MONGODB_RESET_DATABASES = ["library_demo", "education_demo", "benchmark"]
REDIS_RESET_DATABASES = [0, 1, 2]
CASSANDRA_RESET_KEYSPACES = ["library_demo", "education_demo", "benchmark"]
NEO4J_RESET_LABELS = [
    "Book", "Member", "Genre",
    "Student", "Course", "Instructor", "Department", "Branch",
    "User", "Product",
]
