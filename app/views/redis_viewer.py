# app/views/redis_viewer.py
import customtkinter as ctk
from tkinter import messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json
from tksheet import Sheet
from customtkinter import CTkTextbox

from config import PAGE_SIZES, PREVIEW_LIMIT


class RedisContentViewer(ctk.CTkFrame):
    def __init__(self, master, backend, **kwargs):
        super().__init__(master, **kwargs)
        self.backend = backend
        self.current_meta = {}
        self.selected_db_index = None
        self.current_page = 1
        self.current_page_size = PREVIEW_LIMIT
        self.total_records = 0
        self.actual_total_records = 0
        self.keys_cache = []
        self.cache_note = ""
        self.cache_limit = max(PAGE_SIZES) * 20

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

    def set_loading(self, message="Loading..."):
        callback = getattr(self.winfo_toplevel(), "set_loading", None)
        if callback:
            callback(message)

    def clear_loading(self):
        callback = getattr(self.winfo_toplevel(), "clear_loading", None)
        if callback:
            callback()

    def show_database_selector(self):
        self.set_loading("Loading Redis databases...")
        try:
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
        finally:
            self.clear_loading()

    def select_database(self, db_index):
        self.set_loading("Switching Redis database...")
        try:
            self.backend.switch_db(db_index)
            self.selected_db_index = db_index
            self.show_dashboard()
        except Exception as e:
            messagebox.showerror("Error", f"Cannot switch Redis database: {e}")
        finally:
            self.clear_loading()

    def show_dashboard(self):
        self.clear_view()

        self.summary_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.summary_frame.pack(fill="x", padx=10, pady=(10, 6))

        summary_cards = [
            ("Database", "#2C2C2C"),
            ("Total Keys", "#18357E"),
            ("Expiring", "#4EAFFA"),
            ("Persistent", "#058484"),
        ]
        self.summary_value_labels = {}
        for title, color in summary_cards:
            card = ctk.CTkFrame(self.summary_frame, fg_color=color, corner_radius=10)
            card.pack(side="left", expand=True, fill="both", padx=8)
            ctk.CTkLabel(card, text=title, font=("Arial", 12), text_color="white").pack(pady=(6, 0))
            value_label = ctk.CTkLabel(card, text="--", font=("Arial", 18, "bold"), text_color="white")
            value_label.pack(pady=(0, 10))
            self.summary_value_labels[title] = value_label

        top_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        top_frame.pack(fill="x", pady=(5, 2))

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

        ctk.CTkLabel(top_frame, text="Page size:").pack(side="left", padx=(10, 5))
        self.page_size_var = ctk.StringVar(value=str(PREVIEW_LIMIT))
        self.page_size_dropdown = ctk.CTkComboBox(
            top_frame,
            values=[str(size) for size in PAGE_SIZES],
            variable=self.page_size_var,
            width=100,
            command=self.on_page_size_change
        )
        self.page_size_dropdown.pack(side="left", padx=4)

        ctk.CTkButton(top_frame, text="Search", command=self.search_keys).pack(side="left", padx=4)

        action_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        action_frame.pack(fill="x", pady=(0, 6))

        ctk.CTkButton(action_frame, text="Graphs", command=self.show_graphs_popup).pack(side="left", padx=10, pady=6)
        ctk.CTkButton(action_frame, text="Refresh", command=self.show_metadata).pack(side="left", padx=6, pady=6)
        ctk.CTkButton(action_frame, text="Change Database", command=self.show_database_selector).pack(side="right", padx=10, pady=6)

        self.export_btn = ctk.CTkButton(
            action_frame, text="Export Selected", command=self.export_selected, state="disabled"
        )
        self.export_btn.pack(side="right", padx=6, pady=6)

        self.keys_value_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.keys_value_frame.pack(fill="both", expand=True, pady=6)

        self.keys_value_frame.columnconfigure(0, weight=40)
        self.keys_value_frame.columnconfigure(1, weight=60)

        self.keys_frame = ctk.CTkFrame(self.keys_value_frame, fg_color="#FFFFFF")
        self.keys_frame.grid(row=0, column=0, sticky="nsew", padx=5)

        self.list_controls_frame = ctk.CTkFrame(self.keys_frame, fg_color="#FFFFFF")
        self.list_controls_frame.pack(fill="x", pady=(0, 5))

        self.preview_label = ctk.CTkLabel(
            self.list_controls_frame,
            text="",
            anchor="w",
            font=("Arial", 12)
        )
        self.preview_label.pack(side="left", padx=(0, 10))

        self.pagination_frame = ctk.CTkFrame(self.list_controls_frame, fg_color="#FFFFFF")
        self.prev_btn = ctk.CTkButton(self.pagination_frame, text="Previous", width=100, command=self.go_prev_page)
        self.prev_btn.pack(side="left", padx=5)
        self.page_indicator_label = ctk.CTkLabel(self.pagination_frame, text="")
        self.page_indicator_label.pack(side="left", padx=10)
        self.next_btn = ctk.CTkButton(self.pagination_frame, text="Next", width=100, command=self.go_next_page)
        self.next_btn.pack(side="left", padx=5)
        self.pagination_frame.pack(side="right")

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
        self.set_loading("Loading Redis metadata...")
        try:
            self.current_meta = self.backend.get_metadata()
            self.update_stats(self.current_meta)
            self.search_keys(show_loader=False)
        finally:
            self.clear_loading()

    def search_keys(self, show_loader=True):
        if show_loader:
            self.set_loading("Loading Redis keys...")
        try:
            pattern = self.search_entry.get().strip() or "*"
            selected_type = self.type_var.get().strip()
            self.current_page = 1
            self.current_page_size = int(self.page_size_var.get())
            self.load_keys_cache(pattern, selected_type)
            self.render_keys_page()
        finally:
            if show_loader:
                self.clear_loading()

    def load_keys_cache(self, pattern, selected_type):
        keys = self.backend.list_keys(pattern=pattern, limit=self.cache_limit)
        if selected_type:
            keys = [key for key in keys if key.get("type") == selected_type]

        self.keys_cache = keys
        self.actual_total_records = int(self.current_meta.get("total_keys", 0) or 0)
        self.total_records = min(len(self.keys_cache), self.cache_limit)
        self.cache_note = ""
        if self.actual_total_records > self.cache_limit:
            self.cache_note = " Showing first {} of {} keys.".format(self.cache_limit, self.actual_total_records)

    def render_keys_page(self):
        offset = (self.current_page - 1) * self.current_page_size
        page_keys = self.keys_cache[offset: offset + self.current_page_size]
        data = [
            [k["key"], k.get("type"), k.get("ttl"), f'{k.get("size", 0)}B']
            for k in page_keys
        ]

        self.keys_sheet.set_sheet_data(data)
        self.apply_type_colors()
        self.export_btn.configure(state="normal" if data else "disabled")

        start = offset + 1 if self.total_records else 0
        end = min(offset + len(page_keys), self.total_records)
        note_suffix = self.cache_note if self.cache_note else ""
        self.preview_label.configure(
            text="Showing {}-{} of {} keys in DB {}{}".format(
                start,
                end,
                self.total_records,
                self.selected_db_index,
                note_suffix
            )
        )
        self.update_pagination_controls()

    def update_pagination_controls(self):
        if self.total_records <= 0:
            self.pagination_frame.pack_forget()
            return

        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        self.page_indicator_label.configure(text="Page {} of {}".format(self.current_page, total_pages))
        self.prev_btn.configure(state="disabled" if self.current_page <= 1 else "normal")
        self.next_btn.configure(state="disabled" if self.current_page >= total_pages else "normal")
        self.pagination_frame.pack(side="right")

    def on_page_size_change(self, _value):
        self.current_page_size = int(self.page_size_var.get())
        self.current_page = 1
        if self.selected_db_index is not None:
            self.render_keys_page()

    def go_prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.render_keys_page()

    def go_next_page(self):
        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        if self.current_page < total_pages:
            self.current_page += 1
            self.render_keys_page()

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
        self.summary_value_labels["Database"].configure(text="DB {}".format(self.selected_db_index))
        self.summary_value_labels["Total Keys"].configure(text=str(meta.get("total_keys")))
        self.summary_value_labels["Expiring"].configure(text=str(meta.get("expiring_keys")))
        self.summary_value_labels["Persistent"].configure(text=str(meta.get("persistent_keys")))

    def show_graphs_popup(self):
        if not self.current_meta:
            return

        popup = ctk.CTkToplevel(self)
        popup.title("Redis Graphs")
        popup.geometry("980x520")

        header = ctk.CTkFrame(popup, fg_color="#F0F0F0")
        header.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(header, text="Redis Graphs", font=("Arial", 18, "bold")).pack(side="left", padx=10, pady=10)
        ctk.CTkLabel(header, text="Sampled", font=("Arial", 12)).pack(side="right", padx=10, pady=10)

        body = ctk.CTkFrame(popup, fg_color="#FFFFFF")
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
        fig.patch.set_facecolor("#FFFFFF")

        type_data = self.current_meta.get("type_counts", {})
        type_labels = list(type_data.keys())
        type_values = list(type_data.values())
        type_colors = [self.color_map.get(k, "#999999") for k in type_labels]

        if type_labels:
            wedges, _, _ = axes[0].pie(type_values, colors=type_colors, autopct="%1.1f%%", startangle=90)
            axes[0].legend(wedges, type_labels, title="Key Types", loc="center left", bbox_to_anchor=(1, 0.5))
        axes[0].set_title("Key Type Distribution")

        prefix_data = dict(list(self.current_meta.get("top_prefixes", {}).items())[:5])
        prefix_labels = list(prefix_data.keys())
        prefix_values = list(prefix_data.values())
        if prefix_labels:
            wedges, _, _ = axes[1].pie(prefix_values, autopct="%1.1f%%", startangle=90)
            axes[1].legend(wedges, prefix_labels, title="Prefixes", loc="center left", bbox_to_anchor=(1, 0.5))
        axes[1].set_title("Top Prefixes")

        fig.tight_layout()

        canvas = FigureCanvasTkAgg(fig, master=body)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True, padx=10, pady=10)
        ctk.CTkButton(popup, text="Close", command=popup.destroy).pack(pady=(0, 10))
        plt.close(fig)
