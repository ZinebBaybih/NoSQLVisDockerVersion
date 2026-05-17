# app/views/cassandra_viewer.py
import customtkinter as ctk
from tkinter import messagebox
from tkinter import filedialog
from tksheet import Sheet
import matplotlib.pyplot as plt
# from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import csv
import time

from config import PAGE_SIZES, PAGINATION_CACHE_LIMIT, PREVIEW_LIMIT
from utils.benchmark_logger import is_gui_benchmark_enabled, log_metric

SYSTEM_KEYSPACE_PREFIX = "system"


def is_user_keyspace(keyspace):
    return keyspace and not keyspace.startswith(SYSTEM_KEYSPACE_PREFIX)


class CassandraContentViewer(ctk.CTkFrame):
    """Cassandra viewer with dashboard"""

    def __init__(self, master, backend, **kwargs):
        super().__init__(master, **kwargs)
        self.backend = backend  # NoSQLBackend instance
        self.current_page = 1
        self.current_page_size = PREVIEW_LIMIT
        self.total_records = 0
        self.current_keyspace = None
        self.current_table = None
        self.rows_cache = []
        self.loaded_rows = 0
        self.loaded_preview_rows = 0
        self.exact_total_records = None
        self.cache_limit = PAGINATION_CACHE_LIMIT
        self.cache_note = ""
        self.graph1_data = ([], [])
        self.graph2_data = ([], [])

        self.configure(fg_color="#FFFFFF")
        self.pack(fill="both", expand=True, padx=10, pady=10)

        self.user_keyspaces = self.get_keyspaces()
        self.tables_by_keyspace = self.load_tables_by_keyspace(self.user_keyspaces)
                

        # --- GLOBAL STATS BANNER ---
        self.stats_banner = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.stats_banner.pack(fill="x", padx=10, pady=10)

        # CARD 1 - Application Keyspaces
        card1 = ctk.CTkFrame(self.stats_banner, fg_color="#2C2C2C", corner_radius=10)  # light blue
        card1.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card1, text="Keyspaces", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.keyspaces_label = ctk.CTkLabel(card1, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.keyspaces_label.pack(pady=(0, 10))

        # CARD 2 - Tables in Selected Keyspace
        card2 = ctk.CTkFrame(self.stats_banner, fg_color="#18357E", corner_radius=10)  # light green
        card2.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card2, text="Selected Tables", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.selected_tables_label = ctk.CTkLabel(card2, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.selected_tables_label.pack(pady=(0, 10))

        # CARD 3 - Total Application Tables
        card3 = ctk.CTkFrame(self.stats_banner, fg_color="#4EAFFA", corner_radius=10)  # light orange
        card3.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card3, text="Tables", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.user_tables_label = ctk.CTkLabel(card3, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.user_tables_label.pack(pady=(0, 10))

        # CARD 4 - Loaded Rows
        card4 = ctk.CTkFrame(self.stats_banner, fg_color="#058484", corner_radius=10)  # light purple
        card4.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card4, text="Loaded Rows", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.loaded_rows_label = ctk.CTkLabel(card4, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.loaded_rows_label.pack(pady=(0, 10))


        # --- DASHBOARD FRAME (Banner Style) ---
        self.dashboard_frame = ctk.CTkFrame(self, fg_color="#EAEAEA", height=120)
        self.dashboard_frame.pack(fill="x", padx=10, pady=10)

        # Graph 1: bar chart (rows per table)
        self.graph1_frame = ctk.CTkFrame(self.dashboard_frame, fg_color="#F8F8F8")
        self.graph1_frame.pack(side="left", fill="both", expand=True, padx=5, pady=5)

        self.figure1 = plt.Figure(figsize=(6, 3.5), dpi=100)
        # self.figure1 = Figure(figsize=(6, 3.5), dpi=100)
        self.ax1 = self.figure1.add_subplot(111)
        self.ax1.set_title("Rows per table")
        self.canvas1 = FigureCanvasTkAgg(self.figure1, master=self.graph1_frame)
        self.canvas1.get_tk_widget().pack(fill="both", expand=True)

        # Graph 2: second graph (example: table length distribution)
        self.graph2_frame = ctk.CTkFrame(self.dashboard_frame, fg_color="#F8F8F8")
        self.graph2_frame.pack(side="left", fill="both", expand=True, padx=5, pady=5)

        self.figure2 = plt.Figure(figsize=(4, 2), dpi=100)
        # self.figure2 = Figure(figsize=(4,2), dpi=100)
        self.ax2 = self.figure2.add_subplot(111)
        self.ax2.set_title("Rows per keyspace")
        self.canvas2 = FigureCanvasTkAgg(self.figure2, master=self.graph2_frame)
        self.canvas2.get_tk_widget().pack(fill="both", expand=True)
        self.dashboard_frame.pack_forget()



        # ------------------------------------------------------ CONTROLS FRAME ------------------------------------------------------
        self.controls_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        self.controls_frame.pack(fill="x", padx=10, pady=5)

        self.keyspace_var = ctk.StringVar()
        self.keyspace_dropdown = ctk.CTkComboBox(
            self.controls_frame,
            values=self.user_keyspaces,
            variable=self.keyspace_var,
            command=self.on_keyspace_change,
            width=200
        )
        self.keyspace_dropdown.pack(side="left", padx=5)

        self.table_var = ctk.StringVar()
        self.table_dropdown = ctk.CTkComboBox(
            self.controls_frame,
            values=[],
            variable=self.table_var,
            command=self.on_table_change,
            width=200
        )
        self.table_dropdown.pack(side="left", padx=5)

        self.page_size_var = ctk.StringVar(value=str(PREVIEW_LIMIT))

        self.refresh_btn = ctk.CTkButton(
            self.controls_frame, text="Refresh Keyspaces", command=self.refresh_keyspaces
        )
        self.refresh_btn.pack(side="left", padx=5)

        self.graphs_btn = ctk.CTkButton(
            self.controls_frame, text="Graphs", command=self.show_graphs_popup
        )
        self.graphs_btn.pack(side="left", padx=5)

        self.export_btn = ctk.CTkButton(
            self.controls_frame,
            text="Export",
            command=self.export_selected,
            state="disabled"
        )
        self.export_btn.pack(side="right", padx=6)


        # ------------------------------------------------------ SEARCH FRAME ------------------------------------------------------
        self.search_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        self.search_frame.pack(fill="x", padx=10, pady=(0, 5))

        # Column dropdown
        self.column_var = ctk.StringVar()
        self.column_dropdown = ctk.CTkComboBox(
            self.search_frame,
            values=[],  # will populate with selected table columns in on_table_change
            variable=self.column_var,
            width=150
        )
        self.column_dropdown.pack(side="left", padx=5)

        # Operator dropdown
        self.operator_var = ctk.StringVar()
        self.operator_dropdown = ctk.CTkComboBox(
            self.search_frame,
            values=["=", "!=", ">", "<", ">=", "<="],
            variable=self.operator_var,
            width=100
        )
        self.operator_dropdown.pack(side="left", padx=5)

        # Value entry
        self.value_entry = ctk.CTkEntry(self.search_frame, placeholder_text="Value", width=180)
        self.value_entry.pack(side="left", padx=5)

        # Search button
        self.search_btn = ctk.CTkButton(self.search_frame, text="Search", command=self.search_table)
        self.search_btn.pack(side="left", padx=5)

        self.filter_mode_label = ctk.CTkLabel(
            self.search_frame,
            text="",
            width=70,
            anchor="w",
            font=("Arial", 12),
        )
        self.filter_mode_label.pack(side="left", padx=(4, 0))

        ctk.CTkLabel(self.search_frame, text="Page size:").pack(side="left", padx=(16, 5))
        self.page_size_dropdown = ctk.CTkComboBox(
            self.search_frame,
            values=[str(size) for size in PAGE_SIZES],
            variable=self.page_size_var,
            width=100,
            command=self.on_page_size_change
        )
        self.page_size_dropdown.pack(side="left", padx=5)

        # ------------------------------------------------------ SHEET FRAME ------------------------------------------------------
        self.sheet_frame = ctk.CTkFrame(self, fg_color="#F8F8F8")
        self.sheet_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.sheet_frame.pack_propagate(True)

        self.table_controls_frame = ctk.CTkFrame(self.sheet_frame, fg_color="#F8F8F8")
        self.table_controls_frame.pack(fill="x", pady=(0, 5))

        self.preview_label = ctk.CTkLabel(
            self.table_controls_frame,
            text="",
            anchor="w",
            font=("Arial", 12)
        )
        self.preview_label.pack(side="left", padx=(0, 10))
        self.count_btn = ctk.CTkButton(
            self.table_controls_frame,
            text="Calculate Total",
            width=130,
            command=self.calculate_total_count,
            state="disabled"
        )
        self.count_btn.pack(side="left", padx=(0, 10))

        # --- SHEET ---
        self.sheet = Sheet(
            self.sheet_frame,
            show_x_scrollbar=True,
            show_y_scrollbar=True,
            row_height=25,
        )
        self.sheet.pack(fill="both", expand=True)

        # auto resize columns, column minimum width set to 150 pixels
        self.sheet.set_options(
            auto_resize_columns=150,
            header_font=("Arial", 13, "bold"),
            index_font=("Arial", 12, "normal"),
            font=("Arial", 12, "normal"),
        )
        self.sheet.enable_bindings((
            "single_select",
            "row_select",
            "column_select",
            "arrowkeys",
            "row_height_resize",
            "column_width_resize",
            "stretch_column_to_fit" 
        ))

        self.pagination_frame = ctk.CTkFrame(self.table_controls_frame, fg_color="#F8F8F8")
        self.prev_btn = ctk.CTkButton(self.pagination_frame, text="Previous", width=100, command=self.go_prev_page)
        self.prev_btn.pack(side="left", padx=5)
        self.page_indicator_label = ctk.CTkLabel(self.pagination_frame, text="")
        self.page_indicator_label.pack(side="left", padx=10)
        self.next_btn = ctk.CTkButton(self.pagination_frame, text="Next", width=100, command=self.go_next_page)
        self.next_btn.pack(side="left", padx=5)
        self.pagination_frame.pack(side="right")



        # Initialize selections
        if self.user_keyspaces:
            self.set_loading("Loading Cassandra overview...")
            try:
                first = self.user_keyspaces[0]
                self.keyspace_dropdown.set(first)
                self.on_keyspace_change(first, show_loader=False)
                self.update_dashboard()
            finally:
                self.clear_loading()
        else:
            self.update_dashboard()

    def set_loading(self, message="Loading..."):
        callback = getattr(self.winfo_toplevel(), "set_loading", None)
        if callback:
            callback(message)

    def clear_loading(self):
        callback = getattr(self.winfo_toplevel(), "clear_loading", None)
        if callback:
            callback()

    def log_gui_metric(self, metric, start, page_size="", page="", records_returned="", total_records="", status="ok", error=""):
        if not is_gui_benchmark_enabled():
            return
        self.update_idletasks()
        log_metric(
            database="Cassandra",
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

    # -----------------------------
    # Backend helpers
    # -----------------------------
    def get_keyspaces(self):
        try:
            return [
                keyspace
                for keyspace in self.backend.list_keyspaces()
                if is_user_keyspace(keyspace)
            ]
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch keyspaces: {e}")
            return []

    def get_tables(self, keyspace):
        if not is_user_keyspace(keyspace):
            return []
        if keyspace in self.tables_by_keyspace:
            return self.tables_by_keyspace[keyspace]
        try:
            tables = self.backend.list_tables(keyspace)
            self.tables_by_keyspace[keyspace] = tables
            return tables
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch tables: {e}")
            return []

    def load_tables_by_keyspace(self, keyspaces):
        tables_by_keyspace = {}
        for keyspace in keyspaces:
            if not is_user_keyspace(keyspace):
                continue
            try:
                tables_by_keyspace[keyspace] = self.backend.list_tables(keyspace)
            except Exception as e:
                messagebox.showerror("Error", f"Cannot fetch tables for {keyspace}: {e}")
                tables_by_keyspace[keyspace] = []
        return tables_by_keyspace

    def get_sample_data(self, keyspace, table, limit=PREVIEW_LIMIT):
        try:
            return self.backend.fetch_sample(keyspace, table, limit)
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch data: {e}")
            return []

    # -----------------------------
    # UI Callbacks
    # -----------------------------
    def on_keyspace_change(self, value, show_loader=True):
        start = time.perf_counter()
        records_returned = ""
        if show_loader:
            self.set_loading("Loading Cassandra keyspace...")
        try:
            self.export_btn.configure(state="disabled")
            tables = self.get_tables(value)
            records_returned = len(tables)
            self.table_dropdown.configure(values=tables)
            if tables:
                self.table_dropdown.set(tables[0])
                self.on_table_change(tables[0], show_loader=False)
            else:
                self.current_keyspace = value
                self.current_table = None
                self.rows_cache = []
                self.loaded_rows = 0
                self.loaded_preview_rows = 0
                self.exact_total_records = None
                self.sheet.set_sheet_data([])
                self.preview_label.configure(text="Select a table to load preview rows")
                self.count_btn.configure(state="disabled")
                self.pagination_frame.pack_forget()
            self.update_dashboard()
        finally:
            self.log_gui_metric("scope_load", start, records_returned=records_returned)
            if show_loader:
                self.clear_loading()


    def on_table_change(self, value, show_loader=True):
        start = time.perf_counter()
        if show_loader:
            self.set_loading("Loading Cassandra table...")
        try:
            keyspace = self.keyspace_dropdown.get()
            self.current_keyspace = keyspace
            self.current_table = value
            self.total_records = 0
            self.exact_total_records = None
            self.set_filter_mode_label(None)
            self.load_rows_cache()
            self.current_page = 1
            self.current_page_size = int(self.page_size_var.get())
            self.render_table_page(show_loader=False)
            self.update_dashboard()
        finally:
            self.log_gui_metric(
                "metadata_load",
                start,
                records_returned=1 if value else 0,
                total_records=self.loaded_rows,
            )
            if show_loader:
                self.clear_loading()

    def render_table_page(self, show_loader=True):
        start = time.perf_counter()
        rows = []
        if show_loader:
            self.set_loading("Loading Cassandra rows...")
        if not self.current_keyspace or not self.current_table:
            self.pagination_frame.pack_forget()
            if show_loader:
                self.clear_loading()
            return

        try:
            self.set_filter_mode_label(None)
            offset = (self.current_page - 1) * self.current_page_size

            # Cassandra CQL has no native OFFSET. We cache a bounded preview once,
            # then paginate locally so page navigation stays responsive.
            data = self.rows_cache[offset: offset + self.current_page_size]

            if not data:
                self.sheet.set_sheet_data([])
                self.sheet.headers([])
                self.export_btn.configure(state="disabled")
                self.loaded_rows = 0
                self.loaded_preview_rows = 0
                self.loaded_rows_label.configure(text="0")
                self.preview_label.configure(text="Showing 0 rows out of loaded 0")
                self.update_pagination_controls()
                return

            headers = list(data[0].keys())
            rows = [list(row.values()) for row in data]
            self.sheet.headers(headers)
            self.sheet.set_sheet_data(rows)
            self.export_btn.configure(state="normal" if rows else "disabled")
            self.column_dropdown.configure(values=headers)
            self.loaded_rows = len(rows)
            self.loaded_rows_label.configure(text=str(self.loaded_rows))

            start = offset + 1 if self.loaded_preview_rows > 0 else 0
            end = min(offset + len(rows), self.loaded_preview_rows)
            self.preview_label.configure(
                text="Showing {}-{} rows out of loaded {}{}".format(
                    start,
                    end,
                    self.loaded_preview_rows,
                    self.cache_note,
                )
            )
            self.update_pagination_controls()
        finally:
            self.log_gui_metric(
                "first_page" if self.current_page == 1 else "next_page",
                start,
                page_size=self.current_page_size,
                page=self.current_page,
                records_returned=len(rows),
                total_records=self.loaded_preview_rows,
            )
            if show_loader:
                self.clear_loading()

    def update_pagination_controls(self):
        if not self.current_table or not self.rows_cache:
            self.pagination_frame.pack_forget()
            return

        total_pages = max(1, (len(self.rows_cache) + self.current_page_size - 1) // self.current_page_size)
        self.page_indicator_label.configure(text="Page {} of {}".format(self.current_page, total_pages))
        self.prev_btn.configure(state="disabled" if self.current_page <= 1 else "normal")
        self.next_btn.configure(state="disabled" if self.current_page >= total_pages else "normal")
        self.pagination_frame.pack(side="right")

    def load_rows_cache(self):
        self.rows_cache = self.get_sample_data(
            self.current_keyspace,
            self.current_table,
            limit=self.cache_limit
        )
        self.loaded_preview_rows = len(self.rows_cache)
        self.total_records = self.loaded_preview_rows
        self.cache_note = ""
        self.count_btn.configure(state="normal" if self.current_table else "disabled")

    def on_page_size_change(self, _value):
        start = time.perf_counter()
        self.current_page_size = int(self.page_size_var.get())
        if self.current_table:
            self.current_page = 1
            self.render_table_page()
            self.log_gui_metric(
                "page_size_change",
                start,
                page_size=self.current_page_size,
                page=self.current_page,
                total_records=self.loaded_preview_rows,
            )

    def go_prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.render_table_page()

    def go_next_page(self):
        total_pages = max(1, (len(self.rows_cache) + self.current_page_size - 1) // self.current_page_size)
        if self.current_page < total_pages:
            self.current_page += 1
            self.render_table_page()


    def refresh_keyspaces(self):
        self.set_loading("Refreshing Cassandra keyspaces...")
        try:
            self.export_btn.configure(state="disabled")
            self.user_keyspaces = self.get_keyspaces()
            self.tables_by_keyspace = self.load_tables_by_keyspace(self.user_keyspaces)
            
            self.keyspace_dropdown.configure(values=self.user_keyspaces)
            current_keyspace = self.keyspace_var.get()
            if current_keyspace in self.user_keyspaces:
                self.keyspace_dropdown.set(current_keyspace)
                tables = self.get_tables(current_keyspace)
                self.table_dropdown.configure(values=tables)
            elif self.user_keyspaces:
                self.keyspace_dropdown.set(self.user_keyspaces[0])
                tables = self.get_tables(self.user_keyspaces[0])
                self.table_dropdown.configure(values=tables)
                if tables:
                    self.table_dropdown.set(tables[0])
                self.current_keyspace = None
                self.current_table = None
                self.rows_cache = []
                self.loaded_rows = 0
                self.loaded_preview_rows = 0
                self.exact_total_records = None
                self.sheet.set_sheet_data([])
                self.preview_label.configure(text="Select a table to load preview rows")
                self.count_btn.configure(state="disabled")
                self.pagination_frame.pack_forget()
            self.update_dashboard()
        finally:
            self.clear_loading()

    def search_table(self):
        table = self.table_var.get()
        keyspace = self.keyspace_var.get()
        column = self.column_var.get()
        operator = self.operator_var.get()
        value = self.value_entry.get()

        if not table or not column or not operator or not value:
            messagebox.showinfo("Info", "Please select table, column, operator, and enter value")
            return

        headers = list(self.sheet.headers())
        if column not in headers:
            messagebox.showinfo("Info", "Please select a visible column")
            return

        if operator == "=" and self.can_use_native_filter(keyspace, table, column):
            try:
                results, _queryable_columns = self.backend.search_indexed_table(
                    keyspace,
                    table,
                    column,
                    value,
                    limit=self.cache_limit,
                )
                self.set_filter_mode_label("indexed")
                self.render_search_results(results)
                return
            except Exception as e:
                print("Erreur search_indexed_table:", e)

        self.set_filter_mode_label("preview")
        rows = self.sheet.get_sheet_data()
        col_index = headers.index(column)

        def normalize(v):
            if v is None:
                return ("none", None)
            s = str(v).strip().lower()
            try:
                return ("number", float(s))
            except Exception:
                return ("string", s)

        input_type, input_val = normalize(value)

        def compare(row_val):
            row_type, row_norm = normalize(row_val)
            if row_type != input_type:
                return False
            if operator == "=":
                return row_norm == input_val
            if operator == "!=":
                return row_norm != input_val
            if operator == ">":
                return row_norm > input_val
            if operator == "<":
                return row_norm < input_val
            if operator == ">=":
                return row_norm >= input_val
            if operator == "<=":
                return row_norm <= input_val
            return False

        results = [
            dict(zip(headers, row)) for row in rows
            if col_index < len(row) and compare(row[col_index])
        ]

        self.render_search_results(results)
        return

    def can_use_native_filter(self, keyspace, table, column):
        try:
            return column in self.backend.get_queryable_filter_columns(keyspace, table)
        except Exception as e:
            print("Erreur get_queryable_filter_columns:", e)
            return False

    def set_filter_mode_label(self, mode):
        if mode == "indexed":
            self.filter_mode_label.configure(text="Indexed", text_color="#2E8B57")
        elif mode == "preview":
            self.filter_mode_label.configure(text="No index", text_color="#8A6A00")
        else:
            self.filter_mode_label.configure(text="", text_color="#1e1e1e")

    def render_search_results(self, results):
        if not results:
            self.sheet.set_sheet_data([])
            self.export_btn.configure(state="disabled")
            self.loaded_rows = 0
            self.loaded_rows_label.configure(text="0")
            self.preview_label.configure(text="Filter result: 0 matching rows")
            return

    # Convert list of dicts → headers + rows
        headers = list(results[0].keys())
        rows = [list(row.values()) for row in results]

        self.sheet.headers(headers)
        self.sheet.set_sheet_data(rows)
        self.loaded_rows = len(rows)
        self.preview_label.configure(
            text="Filter result: {} matching rows".format(len(rows))
        )
        self.loaded_rows_label.configure(text=str(self.loaded_rows))

        self.export_btn.configure(state="normal")

    # DASHBOARD UPDATE

    def update_dashboard(self):
        self.keyspaces_label.configure(text=f"{len(self.user_keyspaces)}")
        current_ks = self.keyspace_dropdown.get()
        current_tables = self.get_tables(current_ks) if current_ks else []
        self.selected_tables_label.configure(text=f"{len(current_tables)}")

        total_user_tables = sum(len(tables) for tables in self.tables_by_keyspace.values())
        self.user_tables_label.configure(text=f"{total_user_tables}")
        self.loaded_rows_label.configure(text=f"{self.loaded_rows}")

    def show_graphs_popup(self):
        start = time.perf_counter()
        popup = ctk.CTkToplevel(self)
        popup.title("Cassandra Graphs")
        popup.geometry("980x520")

        header = ctk.CTkFrame(popup, fg_color="#F0F0F0")
        header.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(header, text="Cassandra Graphs", font=("Arial", 18, "bold")).pack(side="left", padx=10, pady=10)

        body = ctk.CTkFrame(popup, fg_color="#FFFFFF")
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
        fig.patch.set_facecolor("#FFFFFF")

        selected_keyspace = self.keyspace_dropdown.get()
        table_names = []
        table_row_counts = []
        for table in self.get_tables(selected_keyspace):
            row_count = self.backend.count_rows(selected_keyspace, table)
            if row_count == -1:
                continue
            table_names.append(f"{selected_keyspace}.{table}")
            table_row_counts.append(row_count)

        axes[0].bar(table_names, table_row_counts)
        axes[0].set_title("Rows per Table in {}".format(selected_keyspace))
        axes[0].set_ylabel("Rows")
        axes[0].tick_params(axis="x", rotation=30)
        axes[0].margins(x=0.1)

        ks_names = list(self.tables_by_keyspace.keys())
        ks_table_counts = [len(tables) for tables in self.tables_by_keyspace.values()]
        axes[1].bar(ks_names, ks_table_counts)
        axes[1].set_title("Tables per Keyspace")
        axes[1].set_ylabel("Tables")
        axes[1].tick_params(axis="x", rotation=45)

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=body)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        ctk.CTkButton(popup, text="Close", command=popup.destroy).pack(pady=(0, 10))
        plt.close(fig)
        self.log_gui_metric(
            "graph_prepare",
            start,
            records_returned=len(table_names),
            total_records=sum(table_row_counts) if table_row_counts else "",
        )

    def calculate_total_count(self):
        if not self.current_keyspace or not self.current_table:
            return

        start = time.perf_counter()
        self.set_loading("Counting Cassandra rows...")
        try:
            total = self.backend.count_rows(self.current_keyspace, self.current_table)
            self.exact_total_records = total
            if total == -1:
                messagebox.showerror("Count Rows", "Unable to calculate total rows for this table.")
                return
            self.preview_label.configure(
                text="Showing current page {} rows; loaded {} rows; exact total {} rows".format(
                    self.loaded_rows,
                    self.loaded_preview_rows,
                    total
                )
            )
        finally:
            self.log_gui_metric(
                "count_rows",
                start,
                records_returned=1,
                total_records=self.exact_total_records if self.exact_total_records is not None else "",
            )
            self.clear_loading()


    def export_selected(self):
        keyspace = self.keyspace_var.get()
        table = self.table_var.get()

        if not keyspace or not table:
            return

        # Get headers from the sheet
        headers = self.sheet.headers()  # always get headers from sheet

        data = self.sheet.get_sheet_data()  # current visible page/filter result
        if not data:
            messagebox.showinfo("Export", "No visible rows to export.")
            return

        # Ask filename
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")]
        )
        if not file_path:
            return

        # Write CSV
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)  # always write headers
            writer.writerows(data)

        messagebox.showinfo("Export", "Export complete!")

