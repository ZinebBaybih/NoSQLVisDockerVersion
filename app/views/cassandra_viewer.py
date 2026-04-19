# app/views/cassandra_viewer.py
import customtkinter as ctk
from tkinter import messagebox
from tkinter import filedialog
from tksheet import Sheet
import matplotlib.pyplot as plt
# from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import csv


class CassandraContentViewer(ctk.CTkFrame):
    """Cassandra viewer with dashboard"""

    def __init__(self, master, backend, **kwargs):
        super().__init__(master, **kwargs)
        self.backend = backend  # NoSQLBackend instance

        self.configure(fg_color="#FFFFFF")
        self.pack(fill="both", expand=True, padx=10, pady=10)

        # Excluded Keyspaces
        system_keyspaces = {
            "system", "system_schema", "system_auth",
            "system_traces", "system_distributed",
            "system_virtual_schema"
        }

        # User keyspaces
        all_keyspaces = self.get_keyspaces()
        user_keyspaces = [ks for ks in all_keyspaces if ks not in system_keyspaces]
                

        # --- GLOBAL STATS BANNER ---
        self.stats_banner = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.stats_banner.pack(fill="x", padx=10, pady=10)

        # CARD 1 - System Keyspaces
        card1 = ctk.CTkFrame(self.stats_banner, fg_color="#2C2C2C", corner_radius=10)  # light blue
        card1.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card1, text="System Keyspaces", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.system_ks_label = ctk.CTkLabel(card1, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.system_ks_label.pack(pady=(0, 10))

        # CARD 2 - User Keyspaces
        card2 = ctk.CTkFrame(self.stats_banner, fg_color="#18357E", corner_radius=10)  # light green
        card2.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card2, text="User Keyspaces", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.user_ks_label = ctk.CTkLabel(card2, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.user_ks_label.pack(pady=(0, 10))

        # CARD 3 - Total User Tables
        card3 = ctk.CTkFrame(self.stats_banner, fg_color="#4EAFFA", corner_radius=10)  # light orange
        card3.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card3, text="User Tables", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.user_tables_label = ctk.CTkLabel(card3, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.user_tables_label.pack(pady=(0, 10))

        # CARD 4 - Avg Rows Per Table (Sampled)
        card4 = ctk.CTkFrame(self.stats_banner, fg_color="#058484", corner_radius=10)  # light purple
        card4.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card4, text="Avg Rows/Table", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.avg_rows_label = ctk.CTkLabel(card4, text="0", font=("Arial", 18, "bold"), text_color="white")
        self.avg_rows_label.pack(pady=(0, 10))

        # CARD 5 - Largest Table
        card5 = ctk.CTkFrame(self.stats_banner, fg_color="#FAAA4E", corner_radius=10)  # light red/pink
        card5.pack(side="left", expand=True, fill="both", padx=8)
        ctk.CTkLabel(card5, text="Largest Table", font=("Arial", 12), text_color="white").pack(pady=(5, 0))
        self.largest_table_label = ctk.CTkLabel(card5, text="-", font=("Arial", 10, "bold"), text_color="white")
        self.largest_table_label.pack(pady=(0, 10))


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



        # ------------------------------------------------------ CONTROLS FRAME ------------------------------------------------------
        self.controls_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        self.controls_frame.pack(fill="x", padx=10, pady=5)

        self.keyspace_var = ctk.StringVar()
        self.keyspace_dropdown = ctk.CTkComboBox(
            self.controls_frame,
            values=user_keyspaces,
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

        self.refresh_btn = ctk.CTkButton(
            self.controls_frame, text="Refresh Keyspaces", command=self.refresh_keyspaces
        )
        self.refresh_btn.pack(side="left", padx=5)

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
        self.value_entry = ctk.CTkEntry(self.search_frame, placeholder_text="Value")
        self.value_entry.pack(side="left", padx=5, fill="x", expand=True)

        # Search button
        self.search_btn = ctk.CTkButton(self.search_frame, text="Search", command=self.search_table)
        self.search_btn.pack(side="left", padx=5)


        # ------------------------------------------------------ SHEET FRAME ------------------------------------------------------
        self.sheet_frame = ctk.CTkFrame(self, fg_color="#F8F8F8")
        self.sheet_frame.pack(fill="both", expand=True, padx=10, pady=10)
        self.sheet_frame.pack_propagate(True)

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



        # Initialize selections
        if user_keyspaces:
            first = user_keyspaces[0]
            self.keyspace_dropdown.set(first)
            self.on_keyspace_change(first)

        # Update dashboard
        self.update_dashboard()

    # -----------------------------
    # Backend helpers
    # -----------------------------
    def get_keyspaces(self):
        try:
            return self.backend.list_keyspaces()
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch keyspaces: {e}")
            return []

    def get_tables(self, keyspace):
        try:
            return self.backend.list_tables(keyspace)
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch tables: {e}")
            return []

    def get_sample_data(self, keyspace, table, limit=20):
        try:
            return self.backend.fetch_sample(keyspace, table, limit)
        except Exception as e:
            messagebox.showerror("Error", f"Cannot fetch data: {e}")
            return []

    # -----------------------------
    # UI Callbacks
    # -----------------------------
    def on_keyspace_change(self, value):
        self.export_btn.configure(state="disabled")
        tables = self.get_tables(value)
        self.table_dropdown.configure(values=tables)
        if tables:
            self.table_dropdown.set(tables[0])
            self.on_table_change(tables[0])
        else:
            self.update_dashboard()


    def on_table_change(self, value):
        keyspace = self.keyspace_dropdown.get()
        data = self.get_sample_data(keyspace, value, limit=20)

        if not data:
            self.sheet.set_sheet_data([])
            self.sheet.headers([])
            self.export_btn.configure(state="disabled")
            return

        # Get headers from first row of data
        headers = list(data[0].keys())
        rows = [list(row.values()) for row in data]

        # Set headers in the sheet (tksheet handles headers separately)
        self.sheet.headers(headers)
        self.sheet.set_sheet_data(rows)  # only the data, not headers as first row

        # Enable export button if there are rows
        if rows:
            self.export_btn.configure(state="normal")
        else:
            self.export_btn.configure(state="disabled")
        
        self.column_dropdown.configure(values=headers)

        self.update_dashboard()


    def refresh_keyspaces(self):
        self.export_btn.configure(state="disabled")
        # Excluded Keyspaces
        system_keyspaces = {
            "system", "system_schema", "system_auth",
            "system_traces", "system_distributed",
            "system_virtual_schema"
        }
        # User keyspaces
        all_keyspaces = self.get_keyspaces()
        user_keyspaces = [ks for ks in all_keyspaces if ks not in system_keyspaces]
        
        self.keyspace_dropdown.configure(values=user_keyspaces)
        if user_keyspaces:
            self.keyspace_dropdown.set(user_keyspaces[0])
            self.on_keyspace_change(user_keyspaces[0])

    def search_table(self):
        table = self.table_var.get()
        keyspace = self.keyspace_var.get()
        column = self.column_var.get()
        operator = self.operator_var.get()
        value = self.value_entry.get()

        if not table or not column or not operator or not value:
            messagebox.showinfo("Info", "Please select table, column, operator, and enter value")
            return

        results = self.backend.search_table(keyspace, table, column, operator, value)

        if not results:
            self.sheet.set_sheet_data([])
            self.export_btn.configure(state="disabled")
            return

    # Convert list of dicts → headers + rows
        headers = list(results[0].keys())
        rows = [list(row.values()) for row in results]

        self.sheet.headers(headers)
        self.sheet.set_sheet_data(rows)

        self.export_btn.configure(state="normal")

    # DASHBOARD UPDATE

    def update_dashboard(self):
        system_keyspaces = {
            "system", "system_schema", "system_auth",
            "system_traces", "system_distributed",
            "system_virtual_schema"
        }

        # Separate keyspaces
        all_keyspaces = self.get_keyspaces()
        user_keyspaces = [ks for ks in all_keyspaces if ks not in system_keyspaces]
        system_keyspaces_real = [ks for ks in all_keyspaces if ks in system_keyspaces]

        # Counts
        self.system_ks_label.configure(text=f"{len(system_keyspaces_real)}")
        self.user_ks_label.configure(text=f"{len(user_keyspaces)}")

        # Count user tables
        total_user_tables = sum(len(self.get_tables(ks)) for ks in user_keyspaces)
        self.user_tables_label.configure(text=f"{total_user_tables}")

        # --- Row Sampling Statistics ---
        rows_per_table = []
        largest_table = ("-", 0)

        for ks in user_keyspaces:
            for table in self.get_tables(ks):
                sample = self.get_sample_data(ks, table, limit=50)
                row_count = len(sample)
                rows_per_table.append(row_count)

                if row_count > largest_table[1]:
                    largest_table = (f"{ks}.{table}", row_count)

        # Average rows per table
        avg_rows = int(sum(rows_per_table) / len(rows_per_table)) if rows_per_table else 0
        self.avg_rows_label.configure(text=f"{avg_rows}")

        # Largest table
        self.largest_table_label.configure(
            text=f"{largest_table[0]} ({largest_table[1]} rows)"
        )
        
        # Count tables in current keyspace
        current_ks = self.keyspace_dropdown.get()
        tables = self.get_tables(current_ks)

        # Sample row count of current table
        current_table = self.table_dropdown.get()
        rows = self.get_sample_data(current_ks, current_table, limit=20) if current_table else []

        # --- GRAPH 1: Rows per table ---
        self.ax1.clear()

        table_names = []
        table_row_counts = []

        for ks in user_keyspaces:
            if ks.startswith("system"):
                continue  # ignore system keyspaces
            tables = self.get_tables(ks)

            for t in tables:
                try:
                    sample = self.get_sample_data(ks, t, limit=20)
                    table_names.append(f"{ks}.{t}")
                    table_row_counts.append(len(sample))
                except Exception:
                    # skip problematic tables
                    continue

        self.ax1.bar(table_names, table_row_counts)
        self.ax1.set_title("Rows per Table")
        self.ax1.set_ylabel("Rows")
        self.ax1.tick_params(axis="x", rotation=30)
        self.ax1.margins(x=0.1)

        self.figure1.tight_layout()
        self.canvas1.draw()


        # --- GRAPH 2: Tables per keyspace ---
        self.ax2.clear()

        ks_names = []
        ks_table_counts = []

        for ks in user_keyspaces:
            tables = self.get_tables(ks)
            ks_names.append(ks)
            ks_table_counts.append(len(tables))

        self.ax2.bar(ks_names, ks_table_counts)
        self.ax2.set_title("Tables per Keyspace")
        self.ax2.set_ylabel("Tables")
        self.ax2.tick_params(axis='x', rotation=45)

        self.figure2.tight_layout()
        self.canvas2.draw()


    def export_selected(self):
        keyspace = self.keyspace_var.get()
        table = self.table_var.get()

        if not keyspace or not table:
            return

        # Get selected rows (indexes)
        selected_rows = self.sheet.get_selected_rows()

        # Get headers from the sheet
        headers = self.sheet.headers()  # always get headers from sheet

        # CASE 1: Export selected rows
        if selected_rows:
            data = [self.sheet.get_row_data(r) for r in selected_rows]
        else:
            # CASE 2: Export full sheet
            data = self.sheet.get_sheet_data()  # all rows in the sheet

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

