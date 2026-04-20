# app/views/redis_viewer.py
import customtkinter as ctk
from tkinter import messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json
from tksheet import Sheet
from customtkinter import CTkTextbox

from config import PREVIEW_LIMIT


class RedisContentViewer(ctk.CTkFrame):
    def __init__(self, master, backend, **kwargs):
        super().__init__(master, **kwargs)
        self.backend = backend
        self.chart_canvas = None
        self.graph2_canvas = None
        self.current_meta = {}
        self.selected_db_index = None

        self.color_map = {
            "string": "#DCE9A0",
            "list": "#32CD32",
            "set": "#6460D6",
            "hash": "#A24775",
            "zset": "#CF0377",
            "stream": "#49BFC1"
        }

        self.configure(fg_color="#FFFFFF")
        self.pack(fill="both", expand=True, padx=10, pady=10)

        self.show_database_selector()

    def clear_view(self):
        for widget in self.winfo_children():
            widget.destroy()

    def show_database_selector(self):
        self.clear_view()

        ctk.CTkLabel(
            self,
            text="Select Redis Database",
            font=("Arial", 20, "bold")
        ).pack(pady=(20, 10))

        ctk.CTkLabel(
            self,
            text="Choose a Redis logical database to explore.",
            font=("Arial", 12)
        ).pack(pady=(0, 15))

        selector_frame = ctk.CTkScrollableFrame(self, fg_color="#F8F8F8")
        selector_frame.pack(fill="both", expand=True, padx=20, pady=10)

        try:
            databases = [
                db for db in self.backend.list_databases()
                if db.get("count") and db.get("count") > 0
            ]
        except Exception as e:
            messagebox.showerror("Error", f"Cannot load Redis databases: {e}")
            databases = []

        if not databases:
            ctk.CTkLabel(
                selector_frame,
                text="No non-empty Redis databases found.",
                font=("Arial", 13)
            ).pack(pady=20)
            return

        for db in databases:
            ctk.CTkButton(
                selector_frame,
                text="{} - {} keys".format(db["name"], db["count"]),
                command=lambda index=db["index"]: self.select_database(index),
                height=40
            ).pack(fill="x", padx=10, pady=8)

    def select_database(self, db_index):
        try:
            self.backend.switch_db(db_index)
            self.selected_db_index = db_index
            self.show_dashboard()
        except Exception as e:
            messagebox.showerror("Error", f"Cannot switch Redis database: {e}")

    def show_dashboard(self):
        self.clear_view()

        self.dashboard_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        self.dashboard_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.dashboard_inner_frame = ctk.CTkFrame(self.dashboard_frame, fg_color="#F8F8F8")
        self.dashboard_inner_frame.pack(fill="both", expand=True)

        self.chart_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF")
        self.chart_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        self.stats_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#EFEFEF")
        self.stats_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        self.graph2_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF")
        self.graph2_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        ctk.CTkLabel(self.stats_frame, text="METADATA", font=("Arial", 14, "bold")).pack(pady=(20, 10))

        self.db_total_keys_label = ctk.CTkLabel(self.stats_frame, text="Total keys: --", anchor="w")
        self.db_expiring_keys_label = ctk.CTkLabel(self.stats_frame, text="Expiring keys: --", anchor="w")
        self.db_persistent_keys_label = ctk.CTkLabel(self.stats_frame, text="Persistent keys: --", anchor="w")
        self.db_memory_used_label = ctk.CTkLabel(self.stats_frame, text="Memory used: --", anchor="w")
        self.db_uptime_label = ctk.CTkLabel(self.stats_frame, text="Server uptime: --", anchor="w")
        self.db_other_stat_label = ctk.CTkLabel(self.stats_frame, text="Total key types: --", anchor="w")

        for lbl in (
            self.db_total_keys_label,
            self.db_expiring_keys_label,
            self.db_persistent_keys_label,
            self.db_memory_used_label,
            self.db_uptime_label,
            self.db_other_stat_label,
        ):
            lbl.pack(fill="x", padx=12, pady=4)

        top_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        top_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(
            top_frame,
            text="Exploring DB {}".format(self.selected_db_index),
            font=("Arial", 12, "bold")
        ).pack(side="left", padx=10)

        ctk.CTkLabel(top_frame, text="Search pattern:").pack(side="left", padx=10)
        self.search_entry = ctk.CTkEntry(top_frame, placeholder_text="e.g. user:*")
        self.search_entry.pack(side="left", padx=4)

        ctk.CTkLabel(top_frame, text="Type:").pack(side="left", padx=10)
        self.type_var = ctk.StringVar()
        self.type_dropdown = ctk.CTkComboBox(
            top_frame,
            values=["", "string", "list", "set", "hash", "zset", "stream"],
            variable=self.type_var,
            width=120
        )
        self.type_dropdown.pack(side="left", padx=4)
        self.type_dropdown.set("")

        ctk.CTkButton(top_frame, text="Search", command=self.search_keys).pack(side="left", padx=4)
        ctk.CTkButton(top_frame, text="Refresh", command=self.show_metadata).pack(side="right", padx=6)
        ctk.CTkButton(top_frame, text="Change Database", command=self.show_database_selector).pack(side="right", padx=6)

        self.export_btn = ctk.CTkButton(
            top_frame, text="Export Selected", command=self.export_selected, state="disabled"
        )
        self.export_btn.pack(side="right", padx=6)

        self.preview_label = ctk.CTkLabel(
            self,
            text="",
            anchor="w",
            font=("Arial", 12)
        )
        self.preview_label.pack(fill="x", padx=10, pady=(0, 5))

        self.keys_value_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.keys_value_frame.pack(fill="both", expand=True, pady=6)

        self.keys_value_frame.columnconfigure(0, weight=40)
        self.keys_value_frame.columnconfigure(1, weight=60)

        self.keys_frame = ctk.CTkFrame(self.keys_value_frame, fg_color="#FFFFFF")
        self.keys_frame.grid(row=0, column=0, sticky="nsew", padx=5)

        self.keys_sheet = Sheet(self.keys_frame, headers=["Key", "Type", "TTL", "Size"])
        self.keys_sheet.pack(fill="both", expand=True)

        self.keys_sheet.enable_bindings((
            "single_select",
            "row_select",
            "cell_select",
            "arrowkeys",
            "mouse_select"
        ))

        self.keys_sheet.bind("<ButtonRelease-1>", self.on_row_click)

        self.value_text = CTkTextbox(
            self.keys_value_frame,
            fg_color="#FFFFFF",
            border_width=1,
            border_color="#DDDDDD"
        )
        self.value_text.grid(row=0, column=1, sticky="nsew", padx=5)
        self.value_text.configure(state="disabled")

        self.show_metadata()

    def show_metadata(self):
        self.current_meta = self.backend.get_metadata()
        self.update_pie_chart(self.current_meta)
        self.update_prefix_chart(self.current_meta)
        self.update_stats(self.current_meta)
        self.search_keys()

    def search_keys(self):
        pattern = self.search_entry.get().strip() or "*"
        selected_type = self.type_var.get().strip()

        keys = self.backend.list_keys(pattern=pattern, limit=PREVIEW_LIMIT)
        data = []

        for k in keys:
            if selected_type and k.get("type") != selected_type:
                continue
            data.append([k["key"], k.get("type"), k.get("ttl"), f'{k.get("size", 0)}B'])

        self.keys_sheet.set_sheet_data(data)
        self.apply_type_colors()
        self.export_btn.configure(state="normal" if data else "disabled")

        total_keys = self.current_meta.get("total_keys", 0)
        self.preview_label.configure(
            text="Showing {} of {} keys in DB {}".format(
                len(data),
                total_keys,
                self.selected_db_index
            )
        )

    def on_row_click(self, event=None):
        cell = self.keys_sheet.get_currently_selected()
        if not cell:
            return

        row = cell[0]
        key = self.keys_sheet.get_cell_data(row, 0)
        self.show_key_value(key)

    def show_key_value(self, key):
        kv = self.backend.get_key_value(key)

        self.value_text.configure(state="normal")
        self.value_text.delete("0.0", "end")

        val = kv.get("value")
        if isinstance(val, (dict, list)):
            val = json.dumps(val, indent=2, ensure_ascii=False)

        self.value_text.insert("0.0", str(val))
        self.value_text.configure(state="disabled")

    def apply_type_colors(self):
        for row in range(len(self.keys_sheet.get_sheet_data())):
            key_type = self.keys_sheet.get_cell_data(row, 1)
            color = self.color_map.get(key_type)
            if color:
                self.keys_sheet.highlight_cells(row=row, column=1, bg=color, fg="white")

    def export_selected(self):
        rows = self.keys_sheet.get_selected_rows()
        keys = [self.keys_sheet.get_cell_data(r, 0) for r in rows]
        if not keys:
            return

        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if path:
            self.backend.export_keys_to_csv(keys, path)
            messagebox.showinfo("Export", "Export successful")

    def update_stats(self, meta):
        self.db_total_keys_label.configure(text=f"Total keys: {meta.get('total_keys')}")
        self.db_expiring_keys_label.configure(text=f"Expiring keys: {meta.get('expiring_keys')}")
        self.db_persistent_keys_label.configure(text=f"Persistent keys: {meta.get('persistent_keys')}")
        self.db_memory_used_label.configure(
            text=f"Memory used: {round(meta.get('memory_used', 0) / (1024 * 1024), 2)} MB"
        )

        uptime = meta.get("uptime_seconds", 0)
        self.db_uptime_label.configure(text=f"Server uptime: {uptime // 3600}h")
        self.db_other_stat_label.configure(text=f"Total key types: {len(meta.get('type_counts', {}))}")

    def update_pie_chart(self, meta):
        if self.chart_canvas:
            self.chart_canvas.get_tk_widget().destroy()

        data = meta.get("type_counts", {})
        fig, ax = plt.subplots(figsize=(4, 4))
        labels = list(data.keys())
        values = list(data.values())
        colors = [self.color_map.get(k, "#999999") for k in labels]

        wedges, _, _ = ax.pie(values, colors=colors, autopct="%1.1f%%", startangle=90)
        ax.legend(wedges, labels, title="Key Types", loc="center left", bbox_to_anchor=(1, 0.5))
        ax.set_title("KEY TYPES DISTRIBUTION")

        self.chart_canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        self.chart_canvas.draw()
        self.chart_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)

    def update_prefix_chart(self, meta):
        if self.graph2_canvas:
            self.graph2_canvas.get_tk_widget().destroy()

        data = dict(list(meta.get("top_prefixes", {}).items())[:5])
        fig, ax = plt.subplots(figsize=(4, 4))
        labels = list(data.keys())
        values = list(data.values())

        wedges, _, _ = ax.pie(values, autopct="%1.1f%%", startangle=90)
        ax.legend(wedges, labels, title="Prefixes", loc="center left", bbox_to_anchor=(1, 0.5))
        ax.set_title("TOP KEY PREFIXES")

        self.graph2_canvas = FigureCanvasTkAgg(fig, master=self.graph2_frame)
        self.graph2_canvas.draw()
        self.graph2_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)
