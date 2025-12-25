# app/views/redis_viewer.py
import customtkinter as ctk
from tkinter import ttk, messagebox
from tkinter import filedialog
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
                
        self.configure(fg_color="#FFFFFF")  # main background white
        self.pack(fill="both", expand=True, padx=10, pady=10)

        # header = ctk.CTkLabel(self, text="Redis Content Viewer", font=("Arial", 20, "bold"), fg_color="#FFFFFF", text_color="#000000")
        # header.pack(pady=10)

        # ================================== Dashboard ==================================
        self.color_map = {
            "string": "#FFD700",
            "list": "#32CD32",
            "set": "#FF4500",
            "hash": "#70093C",
            "zset": "#500E8E",
            "stream": "#037779"
        }

        self.dashboard_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")  # light gray dashboard
        self.dashboard_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.dashboard_inner_frame = ctk.CTkFrame(self.dashboard_frame, fg_color="#F8F8F8")  # inner light gray
        self.dashboard_inner_frame.pack(fill="both", expand=True)

        # ------------------------ Column 1: Pie Chart for Key Types
        self.chart_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF", width=240, height=200)
        self.chart_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        # ------------------------ Column 2: Stats labels
        self.stats_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#EFEFEF", width=200, height=200)
        self.stats_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        self.db_total_keys_label = ctk.CTkLabel(self.stats_frame, text="METADATA", anchor="center", text_color="#000000")
        self.db_total_keys_label.pack(fill="x", padx=5, pady=(30,5))

        self.db_total_keys_label = ctk.CTkLabel(self.stats_frame, text="Total keys: --", anchor="w", text_color="#000000")
        self.db_total_keys_label.pack(fill="x", padx=(8,5), pady=4)

        self.db_expiring_keys_label = ctk.CTkLabel(self.stats_frame, text="Expiring keys: --", anchor="w", text_color="#000000")
        self.db_expiring_keys_label.pack(fill="x", padx=(8,5), pady=4)

        self.db_persistent_keys_label = ctk.CTkLabel(self.stats_frame, text="Persistent keys: --", anchor="w", text_color="#000000")
        self.db_persistent_keys_label.pack(fill="x", padx=(8,5), pady=4)

        self.db_memory_used_label = ctk.CTkLabel(self.stats_frame, text="Memory used: --", anchor="w", text_color="#000000")
        self.db_memory_used_label.pack(fill="x", padx=(8,5), pady=4)

        self.db_uptime_label = ctk.CTkLabel(self.stats_frame, text="Server uptime: --", anchor="w", text_color="#000000")
        self.db_uptime_label.pack(fill="x", padx=(8,5), pady=4)

        self.db_other_stat_label = ctk.CTkLabel(self.stats_frame, text="Total key types: --", anchor="w", text_color="#000000")
        self.db_other_stat_label.pack(fill="x", padx=(8,5), pady=4)

        # ------------------------ Column 3: Additional graph frame
        self.graph2_frame = ctk.CTkFrame(self.dashboard_inner_frame, fg_color="#FFFFFF", width=240, height=200)
        self.graph2_frame.pack(side="left", fill="both", expand=True, padx=8, pady=8)

        # ================================== END of Dashboard ! ==================================




        # ====================================== Top controls ====================================
        top_frame = ctk.CTkFrame(self, fg_color="#F0F0F0")
        top_frame.pack(fill="x", pady=5)

        # ---  Prefixes search entry ---
        ctk.CTkLabel(top_frame, text="Search pattern:", fg_color="#F0F0F0", text_color="#000000").pack(side="left", padx=(10, 4))
        self.search_entry = ctk.CTkEntry(top_frame, placeholder_text="e.g. user:*")
        self.search_entry.pack(side="left", padx=4)
        # ---  Type search entry ---
        ctk.CTkLabel(top_frame, text="Type:", fg_color="#F0F0F0", text_color="#000000").pack(side="left", padx=(10,4))
        self.type_var = ctk.StringVar()
        self.type_dropdown = ctk.CTkComboBox(
            top_frame,
            values=["", "string", "list", "set", "hash", "zset", "stream"],
            variable=self.type_var,
            width=120
        )
        self.type_dropdown.pack(side="left", padx=4)
        self.type_dropdown.set("")  # default = all types

        self.search_btn = ctk.CTkButton(top_frame, text="Search", command=self.search_keys)
        self.search_btn.pack(side="left", padx=4)

        self.refresh_btn = ctk.CTkButton(top_frame, text="Refresh", command=self.show_metadata)
        self.refresh_btn.pack(side="right", padx=6)
        self.export_btn = ctk.CTkButton(top_frame, text="Export Selected", command=self.export_selected, state="disabled")
        self.export_btn.pack(side="right", padx=6)

        # ====================================== END of Top controls ====================================

        # ============================= Horizontal frame for keys + values ==============================
        self.keys_value_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.keys_value_frame.pack(fill="both", expand=True, pady=6)

        self.keys_value_frame.columnconfigure(0, weight=40)
        self.keys_value_frame.columnconfigure(1, weight=60)
        self.keys_value_frame.rowconfigure(0, weight=1)

        # ------------------------ KEYS - LEFT SIDE ------------------------
        self.keys_frame = ctk.CTkFrame(self.keys_value_frame, fg_color="#FFFFFF")
        self.keys_frame.grid(row=0, column=0, sticky="nsew", padx=5)

        self.keys_sheet = Sheet(
            self.keys_frame,
            headers=["Key", "Type", "TTL", "Size"],
            show_index=True,
            show_headers=True,
            table_bg="#FFFFFF",
            header_bg="#DDDDDD",
            header_fg="#000000",
            even_bg="#FFFFFF",
            odd_bg="#F9F9F9",
            bg="#FFFFFF",
        )
        self.keys_sheet.pack(fill="both", expand=True)
        self.keys_sheet.set_options(default_bg="#FFFFFF", default_fg="#000000")
        self.keys_sheet.set_options(index_bg="#DDDDDD", index_fg="#000000")
        self.keys_sheet.set_options(
            vertical_scroll_background="#DDDDDD",
            vertical_scroll_relief="flat",
            vertical_scroll_active_bg="#BBBBBB",
            horizontal_scroll_background="#DDDDDD",
            horizontal_scroll_relief="flat",
            horizontal_scroll_active_bg="#BBBBBB"
        )
        self.keys_sheet.enable_bindings((
            "single_select",
            "row_select",
            "cell_select",
            "double_click",
        ))
        self.keys_sheet.bind("<Double-Button-1>", self.on_sheet_double_click)

        # ------------------------ VALUES - RIGHT SIDE ------------------------
        self.value_text = ctk.CTkTextbox(self.keys_value_frame, fg_color="#FFFFFF", text_color="#000000", border_width=1, border_color="#DDDDDD")
        self.value_text.grid(row=0, column=1, sticky="nsew", padx=5)    

        # --- Make readonly (disable typing, pasting, deleting) ---
        self.value_text.bind("<Key>", lambda e: "break")
        self.value_text.bind("<Control-v>", lambda e: "break")
        self.value_text.bind("<Control-x>", lambda e: "break")
        self.value_text.bind("<Control-c>", lambda e: None)  # allow copy
        self.value_text.bind("<Button-1>", lambda e: None)   # allow selection

        # ====================== END Oof Horizontal frame for keys + values =======================


        # initial load
        self.show_metadata()

    # -------------------- metadata & search --------------------
    def show_metadata(self):
        try:
            meta = self.backend.get_metadata()
        except Exception as e:
            messagebox.showerror("Erreur", f"Impossible de charger metadata: {e}")
            return

        # Pie Charts
        self.after(0, lambda: self.update_pie_chart(meta))
        self.after(0, lambda: self.update_prefix_chart(meta))

        # Stats Labels
        self.update_stats(meta)

        # Search Keys
        self.search_keys()


    def search_keys(self):
        pattern = self.search_entry.get().strip() or "*"
        selected_type = self.type_var.get().strip()  # "" means all types
        try:
            keys = self.backend.list_keys(pattern=pattern, limit=10000)
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur recherche clés: {e}")
            return

        data = []
        for k in keys:
            key_str = k["key"]
            ktype = k.get("type")
            ttl = k.get("ttl")
            size = k.get("size", 0)
            size_str = f"{size}B"

             # --- Filter by type if selected ---
            if selected_type and ktype != selected_type:
                continue

            data.append([key_str, ktype, ttl, size_str])

        # --- Update sheet ---
        self.keys_sheet.set_sheet_data(data)
        self.keys_sheet.column_width(0, 300)
        self.keys_sheet.column_width(1, 120)
        self.keys_sheet.column_width(2, 80)
        self.keys_sheet.column_width(3, 80)
        self.keys_sheet.refresh()

        # --- Highlight types ---
        for row_index, row in enumerate(data):
            ktype = row[1]
            if ktype in self.color_map:
                self.keys_sheet.highlight_cells(row=row_index, column=1, bg=self.color_map[ktype], fg="black")

        self.export_btn.configure(state="normal" if keys else "disabled")

    # -------------------- key value view --------------------
    def on_key_double_click(self, event):
        self.view_selected_key()

    def view_selected_key(self):
        selected = self.keys_sheet.get_selected_rows()
        if not selected:
            return
        row_index = selected[0]
        key = self.keys_sheet.get_cell_data(row_index, 0)
        self.selected_key = key
        try:
            kv = self.backend.get_key_value(key)
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur get_key_value: {e}")
            return

        self.value_text.delete("0.0", "end")
        if kv.get("error"):
            self.value_text.insert("0.0", f"Error: {kv['error']}")
            return

        ktype = kv.get("type")
        val = kv.get("value")
        if ktype in ("hash", "stream", "zset", "list", "set"):
            pretty = json.dumps(val, ensure_ascii=False, indent=2)
            self.value_text.insert("0.0", pretty)
        else:
            self.value_text.insert("0.0", str(val))

    def on_sheet_double_click(self, event=None):
        row = self.keys_sheet.get_currently_selected()[0]
        if row is None:
            return
        key = self.keys_sheet.get_cell_data(row, 0)
        self.view_key_from_sheet(key)

    def view_key_from_sheet(self, key):
        try:
            kv = self.backend.get_key_value(key)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to get key value:\n{e}")
            return

        # Enable editing temporarily
        self.value_text.configure(state="normal")
        self.value_text.delete("0.0", "end")

        if kv.get("error"):
            self.value_text.insert("0.0", f"Error: {kv['error']}")
            self.value_text.configure(state="disabled")
            return

        ktype = kv.get("type")
        val = kv.get("value")

        # Format JSON or string value
        if ktype in ("hash", "stream", "zset", "list", "set"):
            pretty = json.dumps(val, ensure_ascii=False, indent=2)
        else:
            pretty = str(val)

        self.value_text.insert("0.0", pretty)

         # Clear previous tags
        self.value_text.tag_remove("key", "1.0", "end")
        self.value_text.tag_remove("value", "1.0", "end")
        self.value_text.tag_remove("brace", "1.0", "end")


        # Regex to color keys and string values
        # This regex captures "key": "value" pairs and standalone strings
        pattern = r'("(.*?)")(\s*:\s*)?("(.*?)")?'
        for m in re.finditer(pattern, pretty):
            # Key
            start = f"1.0 + {m.start(1)} chars"
            end = f"1.0 + {m.end(1)} chars"
            self.value_text.tag_add("key", start, end)

            # Value (if present)
            if m.group(4):
                start = f"1.0 + {m.start(4)} chars"
                end = f"1.0 + {m.end(4)} chars"
                self.value_text.tag_add("value", start, end)

        # Regex for braces: { } [ ]
        for m in re.finditer(r'[\{\}\[\]]', pretty):
            start = f"1.0 + {m.start()} chars"
            end = f"1.0 + {m.end()} chars"
            self.value_text.tag_add("brace", start, end)

        # Configure tag colors
        self.value_text.tag_config("key", foreground="#429ae0")
        self.value_text.tag_config("value", foreground="#c37a57")
        self.value_text.tag_config("brace", foreground="#c1bc02")  



        # --- Make textbox read-only again ---
        self.value_text.configure(state="disabled")


    # -------------------- EXPORT --------------------
    def export_selected(self):
        selected_rows = self.keys_sheet.get_selected_rows()
        if not selected_rows:
            messagebox.showinfo("Info", "No keys selected for export.")
            return
        keys = [self.keys_sheet.get_cell_data(row, 0) for row in selected_rows if self.keys_sheet.get_cell_data(row, 0)]
        if not keys:
            messagebox.showinfo("Info", "Selected rows contain no keys.")
            return
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if not file_path:
            return
        try:
            self.backend.export_keys_to_csv(keys, file_path)
            messagebox.showinfo("Export", f"Export successful:\n{file_path}")
        except Exception as e:
            messagebox.showerror("Error", f"Export failed:\n{e}")

    # -------------------- CHART --------------------

    def update_pie_chart(self, meta):
        if self.chart_canvas:
            self.chart_canvas.get_tk_widget().destroy()

        type_counts = meta.get("type_counts", {})

        if not type_counts:
            for child in self.chart_frame.winfo_children():
                child.destroy()
            ctk.CTkLabel(
                self.chart_frame,
                text="No type data available",
                fg_color="#FFFFFF",
                text_color="#000000"
            ).pack(fill="both", expand=True)
            return

        # --- Dynamically get frame size ---
        self.chart_frame.update_idletasks()
        w = self.chart_frame.winfo_width()
        h = self.chart_frame.winfo_height()

        dpi = 100
        fig_w = max(2, w / dpi)
        fig_h = max(2, h / dpi)

        # --- Create figure ---
        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
        fig.patch.set_facecolor("#FFFFFF")
        ax.set_facecolor("#FFFFFF")

        labels = list(type_counts.keys())
        counts = [type_counts[k] for k in labels]
        colors = [self.color_map.get(lbl, "#000000") for lbl in labels]

        wedges, texts, autotexts = ax.pie(
            counts,
            colors=colors,
            autopct="%1.1f%%",
            startangle=90
        )

        # --- Add legend on the right ---
        ax.legend(
            wedges,
            labels,
            title="Key Types",
            loc="center left",
            bbox_to_anchor=(1.15, 0.5)   # Push legend more to the right
        )

        # --- FORCE space for legend ---
        plt.subplots_adjust(right=0.65)   # Shrink the pie area to free space

        # Alternative:
        # fig.tight_layout(rect=[0, 0, 0.65, 1])

        ax.set_title("KEY TYPES DISTRIBUTION", color="#000000")

        self.chart_canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        self.chart_canvas.draw()
        self.chart_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)


    
    def update_prefix_chart(self, meta):
        if self.graph2_canvas:
            self.graph2_canvas.get_tk_widget().destroy()

        # Prefix data
        top_prefixes = meta.get("top_prefixes", {})
        if not top_prefixes:
            top_prefixes = {
                "user:": 120,
                "session:": 80,
                "order:": 60,
                "cart:": 40,
                "cache:": 30
            }

        labels = list(top_prefixes.keys())
        counts = list(top_prefixes.values())

        # --- Resize based on container ---
        self.graph2_frame.update_idletasks()
        w = self.graph2_frame.winfo_width()
        h = self.graph2_frame.winfo_height()

        dpi = 100
        fig_w = max(2, w / dpi)
        fig_h = max(2, h / dpi)

        # --- Create figure ---
        fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=dpi)
        fig.patch.set_facecolor("#FFFFFF")
        ax.set_facecolor("#FFFFFF")

        wedges, texts, autotexts = ax.pie(
            counts,
            autopct="%1.1f%%",
            startangle=90,
            colors=plt.cm.tab20.colors
        )

        # Legend
        ax.legend(
            wedges,
            labels,
            title="Prefixes",
            loc="center left",
            bbox_to_anchor=(1.15, 0.5)
        )

        # MATCHES PIE CHART 1 — this keeps symmetry
        plt.subplots_adjust(right=0.65)

        ax.set_title("TOP KEY PREFIXES", color="#000000")

        self.graph2_canvas = FigureCanvasTkAgg(fig, master=self.graph2_frame)
        self.graph2_canvas.draw()
        self.graph2_canvas.get_tk_widget().pack(fill="both", expand=True)
        plt.close(fig)



    def update_stats(self, meta):
        # Stats text updates
        self.db_total_keys_label.configure(text=f"Total keys: {meta.get('total_keys', '--')}")
        self.db_expiring_keys_label.configure(text=f"Expiring keys: {meta.get('expiring_keys', '--')}")
        self.db_persistent_keys_label.configure(text=f"Persistent keys: {meta.get('persistent_keys', '--')}")

        # Memory formatting
        mem = meta.get("memory_used", 0)
        mem_mb = round(mem / (1024 * 1024), 2)
        self.db_memory_used_label.configure(text=f"Memory used: {mem_mb} MB")

        # Uptime formatting
        uptime = meta.get("uptime_seconds", 0)
        days = uptime // 86400
        hours = (uptime % 86400) // 3600
        self.db_uptime_label.configure(text=f"Server uptime: {days}d {hours}h")

        self.db_other_stat_label.configure(text=f"Total key types: {len(meta.get('type_counts', {}))}")
