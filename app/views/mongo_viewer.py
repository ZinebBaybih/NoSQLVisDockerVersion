# app/views/mongo_viewer.py
import customtkinter as ctk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from collections import Counter
import csv
import time
from tkinter import filedialog

from config import PAGE_SIZES, PREVIEW_LIMIT
from utils.benchmark_logger import is_gui_benchmark_enabled, log_metric

class MongoContentViewer:
    def __init__(self, parent, backend):
        self.parent = parent
        self.backend = backend
        self.history = []
        self.current_level = 'dbs'
        self.opened_db_btn_frame = None
        self.opened_col_btn_frame = None
        self.current_page = 1
        self.current_page_size = PREVIEW_LIMIT
        self.total_records = 0
        self.current_db_name = None
        self.current_collection_name = None
        self.current_db_collections = []

        # Nettoyage du parent
        for widget in parent.winfo_children():
            widget.destroy()

        # Barre de filtre
        self.filter_var = ctk.StringVar()
        self.filter_entry = ctk.CTkEntry(
            parent,
            placeholder_text="Rechercher / Filtrer...",
            textvariable=self.filter_var,
            height=34
        )
        self.filter_entry.pack(fill="x", padx=20, pady=(15, 10))
        self.filter_var.trace("w", lambda *args: self.update_filter())

        self.summary_frame = ctk.CTkFrame(parent, fg_color="#FFFFFF")
        self.summary_frame.pack(fill="x", padx=20, pady=(0, 8))
        self.summary_cards = {}
        for title, color in [
            ("Database", "#2C2C2C"),
            ("Collections", "#18357E"),
            ("Total Documents", "#4EAFFA"),
            ("Selected Collection", "#058484"),
        ]:
            card = ctk.CTkFrame(self.summary_frame, fg_color=color, corner_radius=10)
            card.pack(side="left", expand=True, fill="both", padx=6)
            ctk.CTkLabel(card, text=title, font=("Arial", 12), text_color="white").pack(pady=(6, 0))
            value = ctk.CTkLabel(card, text="--", font=("Arial", 16, "bold"), text_color="white")
            value.pack(pady=(0, 10))
            self.summary_cards[title] = value
        self.summary_frame.pack_forget()

        self.page_size_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.page_size_frame.pack(fill="x", padx=20, pady=(0, 8))

        ctk.CTkLabel(self.page_size_frame, text="Page size:").pack(side="left", padx=(0, 8))
        self.page_size_var = ctk.StringVar(value=str(PREVIEW_LIMIT))
        self.page_size_dropdown = ctk.CTkComboBox(
            self.page_size_frame,
            values=[str(size) for size in PAGE_SIZES],
            variable=self.page_size_var,
            width=100,
            command=self.on_page_size_change,
        )
        self.page_size_dropdown.pack(side="left")
        self.page_size_frame.pack_forget()

        self.preview_label = ctk.CTkLabel(
            parent,
            text="",
            anchor="w",
            font=ctk.CTkFont(size=12)
        )
        self.preview_label.pack_forget()

        # Zone scrollable
        self.elements_frame = ctk.CTkScrollableFrame(parent, fg_color="transparent")
        self.elements_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))

        self.pagination_frame = ctk.CTkFrame(parent, fg_color="transparent")
        self.prev_btn = ctk.CTkButton(self.pagination_frame, text="Previous", width=100, command=self.go_prev_page)
        self.prev_btn.pack(side="left", padx=5)
        self.page_indicator_label = ctk.CTkLabel(self.pagination_frame, text="")
        self.page_indicator_label.pack(side="left", padx=10)
        self.next_btn = ctk.CTkButton(self.pagination_frame, text="Next", width=100, command=self.go_next_page)
        self.next_btn.pack(side="left", padx=5)
        ctk.CTkLabel(self.pagination_frame, text="Page size:").pack(side="left", padx=(16, 5))
        self.page_size_dropdown_inline = ctk.CTkComboBox(
            self.pagination_frame,
            values=[str(size) for size in PAGE_SIZES],
            variable=self.page_size_var,
            width=100,
            command=self.on_page_size_change,
        )
        self.page_size_dropdown_inline.pack(side="left", padx=5)
        self.pagination_frame.pack_forget()

        # Bouton retour
        self.back_btn = ctk.CTkButton(parent, text="<- Retour", width=90, command=self.go_back)
        self.back_btn.pack_forget()

        # Affichage initial
        self.show_dbs()

    # ---------------------- UTILITAIRES ----------------------
    def clear_elements(self):
        for widget in self.elements_frame.winfo_children():
            widget.destroy()

    def set_loading(self, message="Loading..."):
        callback = getattr(self.parent.winfo_toplevel(), "set_loading", None)
        if callback:
            callback(message)

    def clear_loading(self):
        callback = getattr(self.parent.winfo_toplevel(), "clear_loading", None)
        if callback:
            callback()

    def log_gui_metric(self, metric, start, page_size="", page="", records_returned="", total_records="", status="ok", error=""):
        if not is_gui_benchmark_enabled():
            return
        self.parent.update_idletasks()
        log_metric(
            database="MongoDB",
            layer="gui",
            metric=metric,
            page_size=page_size,
            page=page,
            records_returned=records_returned,
            total_records=total_records,
            duration_ms=(time.perf_counter() - start) * 1000,
            status=status,
            error=error,
        )

    def reset_pagination(self, total_records):
        self.current_page = 1
        self.total_records = int(total_records)
        self.current_page_size = int(self.page_size_var.get())

    def update_pagination_controls(self):
        if self.current_level != "documents" or self.total_records <= 0:
            self.pagination_frame.pack_forget()
            return

        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        self.page_indicator_label.configure(
            text="Page {} of {}".format(self.current_page, total_pages)
        )
        self.prev_btn.configure(state="disabled" if self.current_page <= 1 else "normal")
        self.next_btn.configure(state="disabled" if self.current_page >= total_pages else "normal")
        self.pagination_frame.pack(pady=(0, 5))

    def on_page_size_change(self, _value):
        start = time.perf_counter()
        self.current_page_size = int(self.page_size_var.get())
        if self.current_level == "documents" and self.current_collection_name:
            self.current_page = 1
            self.render_documents_page()
            self.log_gui_metric(
                "page_size_change",
                start,
                page_size=self.current_page_size,
                page=self.current_page,
                total_records=self.total_records,
            )

    def go_prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.render_documents_page()

    def go_next_page(self):
        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        if self.current_page < total_pages:
            self.current_page += 1
            self.render_documents_page()

    def update_filter(self):
        text = self.filter_var.get().lower()
        for frame in self.elements_frame.winfo_children():
            name = getattr(frame, "db_name", getattr(frame, "col_name", getattr(frame, "doc_name", "")))
            if text in name.lower():
                frame.pack(fill="x", pady=5)
            else:
                frame.pack_forget()

    def flatten_dict(self, d, parent_key='', sep='.'):
        """Aplatit les sous-documents pour le CSV."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self.flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, ', '.join(map(str, v))))
            else:
                items.append((new_key, v))
        return dict(items)

    # ---------------------- DATABASES ----------------------
    def show_dbs(self):
        self.set_loading("Loading MongoDB databases...")
        try:
            self.clear_elements()
            self.summary_frame.pack_forget()
            self.preview_label.pack_forget()
            self.page_size_frame.pack_forget()
            self.pagination_frame.pack_forget()
            self.back_btn.pack_forget()
            self.history.clear()
            self.current_level = "dbs"
            self.opened_db_btn_frame = None

            try:
                dbs = self.backend.list_databases()
            except Exception as e:
                print("Erreur list_databases:", e)
                dbs = []

            for db in dbs:
                self.create_db_row(db)
        finally:
            self.clear_loading()

    def update_summary(self, db_name=None, collections_count=0, total_docs=0, selected_collection=None):
        self.summary_cards["Database"].configure(text=db_name or "--")
        self.summary_cards["Collections"].configure(text=str(collections_count) if db_name else "--")
        self.summary_cards["Total Documents"].configure(text=str(total_docs) if db_name else "--")
        self.summary_cards["Selected Collection"].configure(text=selected_collection or "--")
        if db_name:
            self.summary_frame.pack(fill="x", padx=20, pady=(0, 8), before=self.filter_entry)
        else:
            self.summary_frame.pack_forget()

    def create_db_row(self, db):
        frame = ctk.CTkFrame(self.elements_frame, fg_color="#e3e3e3", corner_radius=10)
        frame.pack(fill="x", pady=6, padx=4)
        frame.db_name = db.get("name", "Unknown")

        row = ctk.CTkFrame(frame, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=8)

        name_label = ctk.CTkLabel(
            row,
            text=f" {db.get('name', 'Unknown')} ({db.get('count', 0)} collections)",
            font=ctk.CTkFont(size=14, weight="bold"),
        )
        name_label.pack(side="left", padx=10)

        # Boutons cachés initialement
        btn_frame = ctk.CTkFrame(row, fg_color="transparent")
        btn_frame.pack(side="right")
        btn_frame.pack_forget()

        preview_btn = ctk.CTkButton(
            btn_frame, text="Aperçu", width=80,
            command=lambda d=db["name"]: self.show_collections(d)
        )
        graph_btn = ctk.CTkButton(
            btn_frame, text="Graph", width=80,
            command=lambda d=db["name"]: self.show_db_graph(d)
        )
        export_btn = ctk.CTkButton(
            btn_frame, text="Export", width=80,
            command=lambda d=db["name"]: self.export_database_csv(d)
        )
        preview_btn.pack(side="left", padx=3)
        graph_btn.pack(side="left", padx=3)
        export_btn.pack(side="left", padx=3)

        def toggle_buttons(event=None):
            if self.opened_db_btn_frame and self.opened_db_btn_frame != btn_frame:
                self.opened_db_btn_frame.pack_forget()
            if btn_frame.winfo_manager():
                btn_frame.pack_forget()
                self.opened_db_btn_frame = None
            else:
                btn_frame.pack(side="right")
                self.opened_db_btn_frame = btn_frame

        name_label.bind("<Button-1>", toggle_buttons)
        row.bind("<Button-1>", toggle_buttons)
        frame.bind("<Button-1>", toggle_buttons)

    # ---------------------- COLLECTIONS ----------------------
    def show_collections(self, db_name):
        start = time.perf_counter()
        status = "ok"
        error = ""
        records_returned = ""
        total_records = ""
        self.set_loading("Loading MongoDB collections...")
        try:
            self.clear_elements()
            self.current_db_name = db_name
            self.preview_label.pack_forget()
            self.page_size_frame.pack_forget()
            self.pagination_frame.pack_forget()
            self.current_level = "collections"
            self.history.append(("dbs", None))
            self.back_btn.pack(pady=5, anchor="w")
            self.opened_col_btn_frame = None

            try:
                cols = self.backend.list_collections(db_name)
            except Exception as e:
                print("Erreur list_collections:", e)
                cols = []

            self.current_db_collections = cols
            total_docs = sum(col.get("count", 0) for col in cols)
            records_returned = len(cols)
            total_records = total_docs
            self.update_summary(
                db_name=db_name,
                collections_count=len(cols),
                total_docs=total_docs,
            )

            for col in cols:
                self.create_collection_row(db_name, col)
        except Exception as exc:
            status = "error"
            error = str(exc)
            raise
        finally:
            self.log_gui_metric(
                "scope_load",
                start,
                records_returned=records_returned,
                total_records=total_records,
                status=status,
                error=error,
            )
            self.clear_loading()

    def create_collection_row(self, db_name, col):
        frame = ctk.CTkFrame(self.elements_frame, fg_color="#e3e3e3", corner_radius=10)
        frame.pack(fill="x", pady=6, padx=4)
        frame.col_name = col.get("name", "Unknown")

        row = ctk.CTkFrame(frame, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=8)

        label = ctk.CTkLabel(
            row,
            text=f" {col.get('name', 'Unknown')} ({col.get('count', 0)} documents)",
            font=ctk.CTkFont(size=13)
        )
        label.pack(side="left", padx=10)

        btn_frame = ctk.CTkFrame(row, fg_color="transparent")
        btn_frame.pack(side="right")
        btn_frame.pack_forget()

        view_btn = ctk.CTkButton(
            btn_frame, text="Aperçu", width=80,
            command=lambda c=col["name"], total=col.get("count", 0): self.show_documents(db_name, c, total)
        )
        graph_btn = ctk.CTkButton(
            btn_frame, text="Graph", width=80,
            command=lambda c=col["name"]: self.show_collection_graph(db_name, c)
        )
        export_btn = ctk.CTkButton(
            btn_frame, text="Export", width=80,
            command=lambda c=col["name"]: self.export_collection_csv(db_name, c)
        )
        view_btn.pack(side="left", padx=3)
        graph_btn.pack(side="left", padx=3)
        export_btn.pack(side="left", padx=3)

        def toggle_buttons(event=None):
            if self.opened_col_btn_frame and self.opened_col_btn_frame != btn_frame:
                self.opened_col_btn_frame.pack_forget()
            if btn_frame.winfo_manager():
                btn_frame.pack_forget()
                self.opened_col_btn_frame = None
            else:
                btn_frame.pack(side="right")
                self.opened_col_btn_frame = btn_frame

        label.bind("<Button-1>", toggle_buttons)
        row.bind("<Button-1>", toggle_buttons)
        frame.bind("<Button-1>", toggle_buttons)

    # ---------------------- DOCUMENTS ----------------------
    def show_documents(self, db_name, col_name, total_count=0):
        self.clear_elements()
        self.current_level = "documents"
        self.history.append(("collections", db_name))
        self.back_btn.pack(pady=5, anchor="w")
        self.current_db_name = db_name
        self.current_collection_name = col_name
        total_docs = sum(col.get("count", 0) for col in self.current_db_collections)
        self.update_summary(
            db_name=db_name,
            collections_count=len(self.current_db_collections),
            total_docs=total_docs,
            selected_collection=col_name,
        )
        self.reset_pagination(total_count)
        self.preview_label.pack(fill="x", padx=20, pady=(0, 8))
        self.render_documents_page()

    def render_documents_page(self):
        start = time.perf_counter()
        status = "ok"
        error = ""
        docs = []
        self.set_loading("Loading MongoDB documents...")
        try:
            self.clear_elements()
            offset = (self.current_page - 1) * self.current_page_size
            try:
                docs = self.backend.list_documents(
                    self.current_db_name,
                    self.current_collection_name,
                    offset=offset,
                    limit=self.current_page_size,
                )
            except Exception as e:
                print("Erreur list_documents:", e)
                docs = []

            start = offset + 1 if self.total_records else 0
            end = min(offset + len(docs), self.total_records)
            self.preview_label.configure(
                text="Showing {}-{} of {} documents".format(start, end, self.total_records)
            )

            for doc in docs:
                frame = ctk.CTkFrame(self.elements_frame, fg_color="#e3e3e3", corner_radius=10)
                frame.pack(fill="x", pady=4, padx=6)
                label = ctk.CTkLabel(
                    frame,
                    text=str(doc),
                    justify="left",
                    anchor="w",
                    wraplength=700,
                    font=ctk.CTkFont(size=11)
                )
                label.pack(side="left", padx=10, pady=6)
                frame.doc_name = str(doc)

            self.update_pagination_controls()
        except Exception as exc:
            status = "error"
            error = str(exc)
            raise
        finally:
            self.log_gui_metric(
                "first_page" if self.current_page == 1 else "next_page",
                start,
                page_size=self.current_page_size,
                page=self.current_page,
                records_returned=len(docs),
                total_records=self.total_records,
                status=status,
                error=error,
            )
            self.clear_loading()

    # ---------------------- GRAPHIQUES ----------------------
    def show_db_graph(self, db_name):
        try:
            cols = self.backend.list_collections(db_name)
            labels = [c['name'] for c in cols]
            values = [c['count'] for c in cols]
        except Exception as e:
            print("Erreur graph DB:", e)
            return
        self.show_graph_popup(f"DB: {db_name}", labels, values, "Collections", "Documents")

    def show_collection_graph(self, db_name, col_name):
        try:
            docs = self.backend.list_documents(db_name, col_name)
            keys = [k for doc in docs for k in doc.keys()]
            counter = Counter(keys)
            labels, values = zip(*counter.most_common(10)) if counter else ([], [])
        except Exception as e:
            print("Erreur graph Collection:", e)
            return
        self.show_graph_popup(f"Collection: {col_name}", labels, values, "Champs", "Occurrences")

    def show_graph_popup(self, title, labels, values, xlabel, ylabel):
        start = time.perf_counter()
        if not labels:
            self.show_custom_popup("Aucune donnée à afficher", fg_color="#444")
            return
        popup = ctk.CTkToplevel(self.parent)
        popup.title(title)
        popup.geometry("800x500")
        popup.transient(self.parent.winfo_toplevel())
        popup.attributes("-topmost", True)
        popup.lift()
        popup.focus_force()
        popup.after(300, lambda: popup.attributes("-topmost", False))

        fig, axs = plt.subplots(1,2, figsize=(10,4))
        fig.patch.set_facecolor("#e3e3e3")

        axs[0].bar(labels, values, color="#4fa3ff")
        axs[0].set_title("Diagramme en barres", color="#1e1e1e")
        axs[0].set_xlabel(xlabel, color="#1e1e1e")
        axs[0].set_ylabel(ylabel, color="#1e1e1e")
        axs[0].tick_params(axis='x', rotation=45, colors="#1e1e1e")
        axs[0].tick_params(axis='y', colors="#1e1e1e")

        axs[1].pie(values, labels=labels, autopct="%1.1f%%", colors=plt.cm.Paired.colors)
        axs[1].set_title("Diagramme circulaire")

        for ax in axs:
            ax.set_facecolor("#e3e3e3")

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=popup)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        ctk.CTkButton(popup, text="Fermer", command=popup.destroy).pack(pady=10)
        plt.close(fig)
        self.log_gui_metric("graph_prepare", start, records_returned=len(labels))

    # ---------------------- EXPORTS ----------------------
    def export_database_csv(self, db_name):
        try:
            cols = self.backend.list_collections(db_name)
            filepath = filedialog.asksaveasfilename(
                title="Exporter la base",
                defaultextension=".csv",
                filetypes=[("Fichier CSV", "*.csv")],
                initialfile=f"{db_name}_export.csv"
            )
            if not filepath:
                return
            with open(filepath, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Base", "Collection", "Clé", "Valeur"])
                for col in cols:
                    docs = self.backend.list_documents(
                        db_name,
                        col['name'],
                        offset=0,
                        limit=self.current_page_size,
                    )
                    for doc in docs:
                        flat_doc = self.flatten_dict(doc)
                        for k, v in flat_doc.items():
                            writer.writerow([db_name, col['name'], k, v])
            self.show_custom_popup(f"Export réussi !\nBase '{db_name}' exportée.", fg_color="#2E8B57")
        except Exception as e:
            self.show_custom_popup(f"Erreur : {e}", fg_color="#8B0000")

    def export_collection_csv(self, db_name, col_name):
        try:
            offset = 0
            if self.current_db_name == db_name and self.current_collection_name == col_name:
                offset = (self.current_page - 1) * self.current_page_size
            docs = self.backend.list_documents(
                db_name,
                col_name,
                offset=offset,
                limit=self.current_page_size,
            )
            filepath = filedialog.asksaveasfilename(
                title="Exporter la collection",
                defaultextension=".csv",
                filetypes=[("Fichier CSV", "*.csv")],
                initialfile=f"{db_name}_{col_name}_docs.csv"
            )
            if not filepath:
                return
            all_fields = set()
            flat_docs = []
            for doc in docs:
                flat = self.flatten_dict(doc)
                flat_docs.append(flat)
                all_fields.update(flat.keys())
            all_fields = sorted(all_fields)
            with open(filepath, "w", newline='', encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_fields)
                writer.writeheader()
                for flat_doc in flat_docs:
                    writer.writerow(flat_doc)
            self.show_custom_popup(f"Export réussi !\nCollection '{col_name}' exportée.", fg_color="#2E8B57")
        except Exception as e:
            self.show_custom_popup(f"Erreur : {e}", fg_color="#8B0000")

    # ---------------------- POPUP ----------------------
    def show_custom_popup(self, text, fg_color="#2E8B57"):
        popup = ctk.CTkToplevel(self.parent)
        popup.geometry("300x150")
        popup.title("Notification")
        popup.configure(fg_color=fg_color)
        popup.resizable(False, False)
        label = ctk.CTkLabel(popup, text=text, font=ctk.CTkFont(size=14, weight="bold"), wraplength=260)
        label.pack(expand=True, pady=25)
        popup.transient(self.parent)
        popup.attributes('-topmost', True)
        ctk.CTkButton(popup, text="OK", command=popup.destroy, width=100).pack(pady=10)
        popup.after(5000, popup.destroy)

    # ---------------------- RETOUR ----------------------
    def go_back(self):
        if not self.history:
            return
        last_level, arg = self.history.pop()
        if last_level == "dbs":
            self.show_dbs()
        elif last_level == "collections":
            self.show_collections(arg)
