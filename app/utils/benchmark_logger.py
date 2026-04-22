import csv
import os
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path


BENCHMARK_FIELDS = [
    "timestamp",
    "dataset_size",
    "database",
    "layer",
    "metric",
    "page_size",
    "page",
    "records_returned",
    "total_records",
    "duration_ms",
    "status",
    "error",
]


def is_gui_benchmark_enabled():
    return os.environ.get("NOSQL_VIS_BENCHMARK_UI", "").strip() == "1"


def get_dataset_size(default="unknown"):
    return os.environ.get("NOSQL_VIS_DATASET_SIZE", default).strip() or default


def default_results_path(layer="gui", dataset_size=None):
    dataset = dataset_size or get_dataset_size()
    return Path("results") / "benchmarks" / f"{layer}_{dataset}.csv"


def append_benchmark_row(path, row):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BENCHMARK_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in BENCHMARK_FIELDS})


def log_metric(
    *,
    database,
    layer,
    metric,
    duration_ms,
    dataset_size=None,
    page_size="",
    page="",
    records_returned="",
    total_records="",
    status="ok",
    error="",
    output_path=None,
):
    dataset = dataset_size or get_dataset_size()
    path = output_path or os.environ.get("NOSQL_VIS_BENCHMARK_FILE") or default_results_path(layer, dataset)
    append_benchmark_row(
        path,
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "dataset_size": dataset,
            "database": database,
            "layer": layer,
            "metric": metric,
            "page_size": page_size,
            "page": page,
            "records_returned": records_returned,
            "total_records": total_records,
            "duration_ms": f"{duration_ms:.3f}",
            "status": status,
            "error": error,
        },
    )


@contextmanager
def measure_metric(
    *,
    database,
    layer,
    metric,
    dataset_size=None,
    page_size="",
    page="",
    records_returned="",
    total_records="",
    output_path=None,
    enabled=True,
):
    start = time.perf_counter()
    status = "ok"
    error = ""
    try:
        yield
    except Exception as exc:
        status = "error"
        error = str(exc)
        raise
    finally:
        if enabled:
            duration_ms = (time.perf_counter() - start) * 1000
            log_metric(
                database=database,
                layer=layer,
                metric=metric,
                dataset_size=dataset_size,
                page_size=page_size,
                page=page,
                records_returned=records_returned,
                total_records=total_records,
                duration_ms=duration_ms,
                status=status,
                error=error,
                output_path=output_path,
            )
