# app/views/redis_viewer.py
import customtkinter as ctk
from tkinter import messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import json
from tksheet import Sheet
import re
from customtkinter import CTkTextbox


class RedisContentViewer(ctk.CTkFrame):
    """Redis viewer: show type stats, search keys, view & export key values, and simple charts."""

    def __init__(self, master, backend, **kwargs):
        super().__init__(master, **kwargs)
        self.backend = backend
        self.selected_key = None
        self.chart_canvas = None
        self.graph2_canvas = None

        self.color_map = {
            "string": "#FFD700",
            "list": "#32CD32",
            "set": "#FF4500",
            "hash": "#70093C",
            "zset": "#500E8E",
            "stream": "#037779"
        }

        self.configure(fg_color="#FFFFFF")
        self.pack(fill="both", expand=True, padx=10, pady=10)

        # ===================== DASHBOARD =====================
        self.dashboard_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        self.dashboard_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.dashboard_inner_frame = ctk.CTkFrame(self.dashboard_frame, fg_color="#F8F8F8")
        self.dashboard_inner_frame.pack(fill="both", expand=True)

        self.chart_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF")
        self.chart_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        self.stats_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#EFEFEF")
        self.stats_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        self.db_title = ctk.CTkLabel(self.stats_frame, text="METADATA", text_color="#000000")
        self.db_title.pack(pady=(30, 5))

        self.db_total_keys_label = ctk.CTkLabel(self.stats_frame, text="Total keys: --", anchor="w")
        self.db_total_keys_label.pack(fill="x", padx=8, pady=4)

        self.db_expiring_keys_label = ctk.CTkLabel(self.stats_frame, text="Expiring keys: --", anchor="w")
        self.db_expiring_keys_label.pack(fill="x", padx=8, pady=4)

        self.db_persistent_keys_label = ctk.CTkLabel(self.stats_frame, text="Persistent keys: --", anchor="w")
        self.db_persistent_keys_label.pack(fill="x", padx=8, pady=4)

        self.db_memory_used_label = ctk.CTkLabel(self.stats_frame, text="Memory used: --", anchor="w")
        self.db_memory_used_label.pack(fill="x", padx=8, pady=4)

        self.db_uptime_label = ctk.CTkLabel(self.stats_frame, text="Server uptime: --", anchor="w")
        self.db_uptime_label.pack(fill="x", padx=8, pady=4)

        self.db_other_stat_label = ctk.CTkLabel(self.stats_frame, text="Total key types: --", anchor="w")
        self.db_other_stat_label.pack(fill="x", padx=8, pady=4)

        self.graph2_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF")
        self.graph2_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        # ===================== CONTROLS =====================
        top_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        top_frame.pack(fill="x", pady=5)

        ctk.CTkLabel(top_frame, text="Search pattern:", text_color="#000000").pack(side="left", padx=10)
        self.search_entry = ctk.CTkEntry(top_frame, placeholder_text="e.g. user:*")
        self.search_entry.pack(side="left", padx=4)

        ctk.CTkLabel(top_frame, text="Type:", text_color="#000000").pack(side="left", padx=10)
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

        self.export_btn = ctk.CTkButton(top_frame, text="Export Selected", command=self.export_selected, state="disabled")
        self.export_btn.pack(side="right", padx=6)

        # ===================== KEYS / VALUES =====================
        self.keys_value_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.keys_value_frame.pack(fill="both", expand=True, pady=6)

        self.keys_value_frame.columnconfigure(0, weight=40)
        self.keys_value_frame.columnconfigure(1, weight=60)

        self.keys_frame = ctk.CTkFrame(self.keys_value_frame, fg_color="#FFFFFF")
        self.keys_frame.grid(row=0, column=0, sticky="nsew", padx=5)

        # ---- SAFE tksheet constructor (OLD VERSION COMPATIBLE) ----
        self.keys_sheet = Sheet(
            self.keys_frame,
            headers=["Key", "Type", "TTL", "Size"]
        )
        self.keys_sheet.pack(fill="both", expand=True)

        # Styling applied AFTER creation (safe)
        try:
            self.keys_sheet.set_options(
                default_bg="#FFFFFF",
                default_fg="#000000",
                header_bg="#DDDDDD",
                header_fg="#000000",
                index_bg="#DDDDDD",
                index_fg="#000000",
                even_bg="#FFFFFF",
                odd_bg="#F9F9F9",
            )
        except Exception:
            pass

        self.keys_sheet.enable_bindings((
            "single_select",
            "row_select",
            "cell_select",
            "double_click",
        ))
        self.keys_sheet.bind("<Double-Button-1>", self.on_sheet_double_click)

        self.value_text = CTkTextbox(
            self.keys_value_frame,
            fg_color="#FFFFFF",
            text_color="#000000",
            border_width=1,
            border_color="#DDDDDD"
        )
        self.value_text.grid(row=0, column=1, sticky="nsew", padx=5)
        self.value_text.configure(state="disabled")

        self.show_metadata()

    # ===================== DATA =====================

    def show_metadata(self):
        try:
            meta = self.backend.get_metadata()
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
            return

        self.after(0, lambda: self.update_pie_chart(meta))
        self.after(0, lambda: self.update_prefix_chart(meta))
        self.update_stats(meta)
        self.search_keys()

    def search_keys(self):
        pattern = self.search_entry.get().strip() or "*"
        selected_type = self.type_var.get().strip()

        try:
            keys = self.backend.list_keys(pattern=pattern, limit=10000)
        except Exception as e:
            messagebox.showerror("Erreur", str(e))
            return

        data = []
        for k in keys:
            if selected_type and k.get("type") != selected_type:
                continue
            data.append([k["key"], k.get("type"), k.get("ttl"), f"{k.get('size', 0)}B"])

        self.keys_sheet.set_sheet_data(data)
        self.export_btn.configure(state="normal" if data else "disabled")

    def on_sheet_double_click(self, event=None):
        row = self.keys_sheet.get_selected_rows()[0]
        key = self.keys_sheet.get_cell_data(row, 0)
        self.view_key_from_sheet(key)

    def view_key_from_sheet(self, key):
        try:
            kv = self.backend.get_key_value(key)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return

        self.value_text.configure(state="normal")
        self.value_text.delete("0.0", "end")

        val = kv.get("value")
        if isinstance(val, (dict, list)):
            val = json.dumps(val, indent=2, ensure_ascii=False)

        self.value_text.insert("0.0", str(val))
        self.value_text.configure(state="disabled")

    def export_selected(self):
        rows = self.keys_sheet.get_selected_rows()
        keys = [self.keys_sheet.get_cell_data(r, 0) for r in rows]
        if not keys:
            return

        path = filedialog.asksaveasfilename(defaultextension=".csv")
        if not path:
            return

        try:
            self.backend.export_keys_to_csv(keys, path)
            messagebox.showinfo("Export", "Export successful.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # ===================== DASHBOARD HELPERS =====================

    def update_stats(self, meta):
        self.db_total_keys_label.configure(text=f"Total keys: {meta.get('total_keys', '--')}")
        self.db_expiring_keys_label.configure(text=f"Expiring keys: {meta.get('expiring_keys', '--')}")
        self.db_persistent_keys_label.configure(text=f"Persistent keys: {meta.get('persistent_keys', '--')}")

        mem = meta.get("memory_used", 0)
        self.db_memory_used_label.configure(text=f"Memory used: {round(mem / (1024 * 1024), 2)} MB")

        uptime = meta.get("uptime_seconds", 0)
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
        self.db_uptime_label.configure(text=f"Server uptime: {days}d {hours}h")

        self.db_other_stat_label.configure(text=f"Total key types: {len(meta.get('type_counts', {}))}")

    def update_pie_chart(self, meta):
        if self.chart_canvas:
            self.chart_canvas.get_tk_widget().destroy()

        data = meta.get("type_counts", {})
        if not data:
            return

        fig, ax = plt.subplots(figsize=(4, 4))
        labels = list(data.keys())
        values = list(data.values())
        colors = [self.color_map.get(k, "#999999") for k in labels]

        ax.pie(values, labels=labels, colors=colors, autopct="%1.1f%%", startangle=90)
        ax.set_title("KEY TYPES DISTRIBUTION")

        self.chart_canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        self.chart_canvas.draw()
        self.chart_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)

    def update_prefix_chart(self, meta):
        if self.graph2_canvas:
            self.graph2_canvas.get_tk_widget().destroy()

        data = meta.get("top_prefixes", {})
        if not data:
            return

        fig, ax = plt.subplots(figsize=(4, 4))
        labels = list(data.keys())
        values = list(data.values())

        ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=90)
        ax.set_title("TOP KEY PREFIXES")

        self.graph2_canvas = FigureCanvasTkAgg(fig, master=self.graph2_frame)
        self.graph2_canvas.draw()
        self.graph2_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)
