# app/views/mongo_viewer.py
import customtkinter as ctk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib.pyplot as plt
from collections import Counter
import csv
from tkinter import filedialog

from config import PREVIEW_LIMIT

class MongoContentViewer:
    def __init__(self, parent, backend):
        self.parent = parent
        self.backend = backend
        self.history = []
        self.current_level = 'dbs'
        self.opened_db_btn_frame = None
        self.opened_col_btn_frame = None

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

        # Bouton retour
        self.back_btn = ctk.CTkButton(parent, text="<- Retour", width=90, command=self.go_back)
        self.back_btn.pack_forget()

        # Affichage initial
        self.show_dbs()

    # ---------------------- UTILITAIRES ----------------------
    def clear_elements(self):
        for widget in self.elements_frame.winfo_children():
            widget.destroy()

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
        self.clear_elements()
        self.preview_label.pack_forget()
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
        self.clear_elements()
        self.preview_label.pack_forget()
        self.current_level = "collections"
        self.history.append(("dbs", None))
        self.back_btn.pack(pady=5, anchor="w")
        self.opened_col_btn_frame = None

        try:
            cols = self.backend.list_collections(db_name)
        except Exception as e:
            print("Erreur list_collections:", e)
            cols = []

        for col in cols:
            self.create_collection_row(db_name, col)

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

        try:
            docs = self.backend.list_documents(db_name, col_name)
        except Exception as e:
            print("Erreur list_documents:", e)
            docs = []

        self.preview_label.configure(
            text="Showing {} of {} documents".format(min(len(docs), PREVIEW_LIMIT), total_count)
        )
        self.preview_label.pack(fill="x", padx=20, pady=(0, 8))

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
        if not labels:
            self.show_custom_popup("Aucune donnée à afficher", fg_color="#444")
            return
        popup = ctk.CTkToplevel(self.parent)
        popup.title(title)
        popup.geometry("800x500")

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
                    docs = self.backend.list_documents(db_name, col['name'])
                    for doc in docs:
                        flat_doc = self.flatten_dict(doc)
                        for k, v in flat_doc.items():
                            writer.writerow([db_name, col['name'], k, v])
            self.show_custom_popup(f"Export réussi !\nBase '{db_name}' exportée.", fg_color="#2E8B57")
        except Exception as e:
            self.show_custom_popup(f"Erreur : {e}", fg_color="#8B0000")

    def export_collection_csv(self, db_name, col_name):
        try:
            docs = self.backend.list_documents(db_name, col_name)
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
