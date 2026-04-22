import argparse
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from config import PREVIEW_LIMIT
from models.backend import NoSQLBackend
from utils.benchmark_logger import log_metric


PAGE_SIZES_TO_TEST = [PREVIEW_LIMIT, 1000]


def timed_call(fn):
    start = time.perf_counter()
    result = fn()
    return result, (time.perf_counter() - start) * 1000


def record(args, database, metric, duration_ms, **kwargs):
    log_metric(
        database=database,
        layer="backend",
        metric=metric,
        dataset_size=args.size_label,
        duration_ms=duration_ms,
        output_path=args.output,
        **kwargs,
    )


def safe_len(value):
    try:
        return len(value)
    except Exception:
        return ""


def connect_backend(args, database):
    defaults = {
        "MongoDB": 27017,
        "Redis": 6379,
        "Cassandra": 9042,
        "Neo4j": 7687,
    }
    user = args.neo4j_user if database == "Neo4j" else None
    password = args.neo4j_password if database == "Neo4j" else None
    backend = NoSQLBackend(
        db_type=database,
        host=args.host,
        port=defaults[database],
        user=user,
        password=password,
    )
    _, duration = timed_call(backend.connect)
    record(args, database, "connect", duration)
    return backend


def benchmark_mongodb(args):
    db = "MongoDB"
    backend = connect_backend(args, db)
    try:
        collections, duration = timed_call(lambda: backend.list_collections("benchmark"))
        total = sum(item.get("count", 0) for item in collections)
        record(args, db, "scope_load", duration, records_returned=len(collections), total_records=total)

        _, duration = timed_call(lambda: sum(item.get("count", 0) for item in backend.list_collections("benchmark")))
        record(args, db, "metadata_load", duration, records_returned=len(collections), total_records=total)

        users_total = next((item.get("count", 0) for item in collections if item.get("name") == "users"), "")
        for page_size in PAGE_SIZES_TO_TEST:
            docs, duration = timed_call(lambda size=page_size: backend.list_documents("benchmark", "users", offset=0, limit=size))
            record(args, db, "first_page", duration, page_size=page_size, page=1, records_returned=len(docs), total_records=users_total)

            docs, duration = timed_call(lambda size=page_size: backend.list_documents("benchmark", "users", offset=size, limit=size))
            record(args, db, "next_page", duration, page_size=page_size, page=2, records_returned=len(docs), total_records=users_total)

        _, duration = timed_call(lambda: [doc.keys() for doc in backend.list_documents("benchmark", "users", limit=PREVIEW_LIMIT)])
        record(args, db, "graph_prepare", duration, page_size=PREVIEW_LIMIT, records_returned=PREVIEW_LIMIT, total_records=users_total)
    finally:
        backend.disconnect()


def benchmark_redis(args):
    db = "Redis"
    backend = connect_backend(args, db)
    try:
        dbs, duration = timed_call(backend.list_databases)
        db1_total = next((item.get("count", 0) for item in dbs if item.get("index") == 1), "")
        record(args, db, "scope_load", duration, records_returned=len(dbs), total_records=db1_total)

        _, duration = timed_call(lambda: backend.switch_db(1))
        record(args, db, "scope_select", duration, total_records=db1_total)

        meta, duration = timed_call(backend.get_metadata)
        total = meta.get("total_keys", db1_total)
        record(args, db, "metadata_load", duration, records_returned=meta.get("sampled_keys", ""), total_records=total)

        for page_size in PAGE_SIZES_TO_TEST:
            keys, duration = timed_call(lambda size=page_size: backend.list_keys(pattern="*", limit=size))
            record(args, db, "first_page", duration, page_size=page_size, page=1, records_returned=len(keys), total_records=total)

            keys, duration = timed_call(lambda size=page_size: backend.list_keys(pattern="*", limit=size * 2))
            record(args, db, "next_page", duration, page_size=page_size, page=2, records_returned=max(0, len(keys) - page_size), total_records=total)

        _, duration = timed_call(backend.get_metadata)
        record(args, db, "graph_prepare", duration, records_returned=meta.get("sampled_keys", ""), total_records=total)
    finally:
        backend.disconnect()


def benchmark_cassandra(args):
    db = "Cassandra"
    backend = connect_backend(args, db)
    try:
        tables, duration = timed_call(lambda: backend.list_tables("benchmark"))
        record(args, db, "scope_load", duration, records_returned=len(tables), total_records=len(tables))

        counts, duration = timed_call(lambda: {table: backend.count_rows("benchmark", table) for table in tables})
        total = sum(count for count in counts.values() if count != -1)
        record(args, db, "metadata_load", duration, records_returned=len(counts), total_records=total)

        users_total = counts.get("users", "")
        for page_size in PAGE_SIZES_TO_TEST:
            rows, duration = timed_call(lambda size=page_size: backend.fetch_sample("benchmark", "users", limit=size))
            record(args, db, "first_page", duration, page_size=page_size, page=1, records_returned=len(rows), total_records=users_total)

            rows, duration = timed_call(lambda size=page_size: backend.fetch_sample("benchmark", "users", limit=size * 2))
            record(args, db, "next_page", duration, page_size=page_size, page=2, records_returned=max(0, len(rows) - page_size), total_records=users_total)

        _, duration = timed_call(lambda: {table: backend.count_rows("benchmark", table) for table in tables})
        record(args, db, "graph_prepare", duration, records_returned=len(tables), total_records=total)
    finally:
        backend.disconnect()


def benchmark_neo4j(args):
    db = "Neo4j"
    backend = connect_backend(args, db)
    try:
        labels, duration = timed_call(backend.list_databases)
        user_total = next((item.get("count", 0) for item in labels if item.get("name") == "User"), "")
        record(args, db, "scope_load", duration, records_returned=len(labels), total_records=user_total)

        _, duration = timed_call(backend.list_databases)
        record(args, db, "metadata_load", duration, records_returned=len(labels), total_records=user_total)

        for page_size in PAGE_SIZES_TO_TEST:
            nodes, duration = timed_call(lambda size=page_size: backend.list_documents(None, "User", offset=0, limit=size))
            record(args, db, "first_page", duration, page_size=page_size, page=1, records_returned=len(nodes), total_records=user_total)

            nodes, duration = timed_call(lambda size=page_size: backend.list_documents(None, "User", offset=size, limit=size))
            record(args, db, "next_page", duration, page_size=page_size, page=2, records_returned=len(nodes), total_records=user_total)

        rels, duration = timed_call(lambda: backend.client.list_relationships("User"))
        record(args, db, "graph_prepare", duration, page_size=PREVIEW_LIMIT, records_returned=safe_len(rels), total_records=user_total)
    finally:
        backend.disconnect()


def run_once(args):
    benchmarks = [
        benchmark_mongodb,
        benchmark_redis,
        benchmark_cassandra,
        benchmark_neo4j,
    ]
    for benchmark in benchmarks:
        try:
            benchmark(args)
        except Exception as exc:
            database = benchmark.__name__.replace("benchmark_", "").title()
            record(args, database, "benchmark_error", 0, status="error", error=str(exc))


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark NoSQL Vis backend operations.")
    parser.add_argument("--size-label", required=True, help="Dataset label, e.g. 1K, 10K, 100K.")
    parser.add_argument("--repeats", type=int, default=5, help="Number of benchmark repeats.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="password")
    parser.add_argument(
        "--output",
        default=None,
        help="CSV output path. Defaults to results/benchmarks/backend_<size-label>.csv.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if args.output is None:
        args.output = str(Path("results") / "benchmarks" / f"backend_{args.size_label}.csv")

    for repeat in range(1, args.repeats + 1):
        print(f"Backend benchmark repeat {repeat}/{args.repeats} for {args.size_label}")
        run_once(args)

    print(f"Results written to {args.output}")


if __name__ == "__main__":
    main()
