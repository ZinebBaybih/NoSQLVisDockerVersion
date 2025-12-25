# app/utils/exporters.py
import csv
from tkinter import filedialog

def export_collection_csv(backend, collection_name):
    filepath = filedialog.asksaveasfilename(title="Exporter collection", defaultextension=".csv")
    if not filepath:
        return
    docs = backend.list_documents("DB 0", collection_name)
    keys = set()
    for d in docs:
        keys.update(d.keys())
    keys = sorted(keys)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for d in docs:
            writer.writerow(d)

def export_database_csv(backend, db_name):
    filepath = filedialog.asksaveasfilename(title="Exporter database", defaultextension=".csv")
    if not filepath:
        return
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Collection", "Key", "Value"])
        collections = backend.list_collections(db_name)
        for col in collections:
            docs = backend.list_documents(db_name, col["name"])
            for doc in docs:
                for k, v in doc.items():
                    writer.writerow([col["name"], k, v])

def export_label_csv(backend, label_name):
    filepath = filedialog.asksaveasfilename(title="Exporter label", defaultextension=".csv")
    if not filepath:
        return
    docs = backend.list_documents(label_name, label_name)
    keys = set()
    for d in docs:
        keys.update(d.keys())
    keys = sorted(keys)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for d in docs:
            writer.writerow(d)
