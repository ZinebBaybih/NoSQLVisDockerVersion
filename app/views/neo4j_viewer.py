# app/views/neo4j_viewer.py
import customtkinter as ctk
from tkinter import ttk, messagebox
import csv
import time
from tkinter import filedialog
import matplotlib.pyplot as plt
import networkx as nx

from config import NEO4J_GRAPH_PREVIEW_LIMIT, PAGE_SIZES, PREVIEW_LIMIT
from utils.benchmark_logger import is_gui_benchmark_enabled, log_metric

class Neo4jContentViewer(ctk.CTkFrame):
    """Enhanced Neo4j viewer with node & relationship statistics."""

    def __init__(self, master, backend, **kwargs):
        # super().__init__(master, **kwargs)
        super().__init__(master, fg_color="#ffffff", **kwargs)  # background color
        self.backend = backend
        self.selected_label = None
        self.selected_total_nodes = 0
        self.label_counts = {}
        self.current_page = 1
        self.current_page_size = PREVIEW_LIMIT
        self.total_records = 0
        self.label_properties = []
        self.indexed_filter_properties = []
        self.filter_mode = None
        self.graph_metadata = {}
        self.selected_database_summary = "--"

        self.pack(fill="both", expand=True, padx=10, pady=10)

        self.summary_frame = ctk.CTkFrame(self, fg_color="#FFFFFF")
        self.summary_frame.pack(fill="x", pady=(0, 8))
        self.summary_cards = {}
        for title, color in [
            ("Labels", "#2C2C2C"),
            ("Total Nodes", "#18357E"),
            ("Relationships", "#4EAFFA"),
            ("Selected", "#058484"),
        ]:
            card = ctk.CTkFrame(self.summary_frame, fg_color=color, corner_radius=10)
            card.pack(side="left", expand=True, fill="both", padx=6)
            ctk.CTkLabel(card, text=title, font=("Arial", 12), text_color="white").pack(pady=(6, 0))
            value = ctk.CTkLabel(card, text="--", font=("Arial", 16, "bold"), text_color="white")
            value.pack(pady=(0, 10))
            self.summary_cards[title] = value

        # Table for databases / labels
        self.labels_tree = ttk.Treeview(
            self,
            columns=("label", "nodes", "rels"),
            show="headings",
            height=8
        )
        self.labels_tree.heading("label", text="Database")
        self.labels_tree.heading("nodes", text="Nodes Count")
        self.labels_tree.heading("rels", text="Relations Count")

        self.labels_tree.column("label", anchor="w", width=250)
        self.labels_tree.column("nodes", anchor="center", width=150)
        self.labels_tree.column("rels", anchor="center", width=150)

        self.labels_tree.pack(fill="x", pady=5)
        self.labels_tree.bind("<Double-1>", self.on_label_double_click)

        btn_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        btn_frame.pack(fill="x", pady=10)

        self.export_btn = ctk.CTkButton(btn_frame, text="Export", command=self.export_label, state="disabled")
        self.export_btn.pack(side="left", padx=10)

        self.graph_btn = ctk.CTkButton(btn_frame, text="Graph", command=self.show_graph, state="disabled")
        self.graph_btn.pack(side="left", padx=10)

        self.graph_note_label = ctk.CTkLabel(
            btn_frame,
            text="Graph preview - sampled 2-hop neighborhood, up to {} relationships shown".format(NEO4J_GRAPH_PREVIEW_LIMIT),
            anchor="w",
            font=("Arial", 12)
        )
        self.graph_note_label.pack(side="left", padx=10)

        self.page_size_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        self.page_size_frame.pack_forget()
        self.page_size_var = ctk.StringVar(value=str(PREVIEW_LIMIT))

        self.filter_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        self.filter_frame.pack(fill="x", pady=(0, 6))

        ctk.CTkLabel(self.filter_frame, text="Property:").pack(side="left", padx=(0, 5))
        self.filter_property_var = ctk.StringVar()
        self.filter_property_dropdown = ctk.CTkComboBox(
            self.filter_frame,
            values=[],
            variable=self.filter_property_var,
            width=160,
            state="disabled",
        )
        self.filter_property_dropdown.pack(side="left", padx=5)

        self.filter_value_entry = ctk.CTkEntry(
            self.filter_frame,
            placeholder_text="Value",
            width=180,
            state="disabled",
        )
        self.filter_value_entry.pack(side="left", padx=5)

        self.search_btn = ctk.CTkButton(
            self.filter_frame,
            text="Search",
            width=80,
            command=self.apply_filter,
            state="disabled",
        )
        self.search_btn.pack(side="left", padx=5)

        self.refresh_btn = ctk.CTkButton(
            self.filter_frame,
            text="Refresh",
            width=85,
            command=self.refresh_current_view,
            state="disabled",
        )
        self.refresh_btn.pack(side="left", padx=5)

        self.filter_mode_label = ctk.CTkLabel(
            self.filter_frame,
            text="",
            width=70,
            anchor="w",
            font=("Arial", 12),
        )
        self.filter_mode_label.pack(side="left", padx=(4, 0))

        self.node_controls_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        self.node_controls_frame.pack(fill="x", pady=(0, 4))

        self.preview_label = ctk.CTkLabel(self.node_controls_frame, text="", anchor="w", font=("Arial", 12))
        self.preview_label.pack(side="left", fill="x", expand=True, padx=(0, 10))

        # ---------------- Node Details ----------------
        self.nodes_tree = ttk.Treeview(self, show="headings")
        self.nodes_tree.pack(fill="both", expand=True, pady=5)

        self.pagination_frame = ctk.CTkFrame(self.node_controls_frame, fg_color="#ffffff")
        self.prev_btn = ctk.CTkButton(self.pagination_frame, text="Previous", width=100, command=self.go_prev_page)
        self.prev_btn.pack(side="left", padx=5)
        self.page_indicator_label = ctk.CTkLabel(self.pagination_frame, text="")
        self.page_indicator_label.pack(side="left", padx=10)
        self.next_btn = ctk.CTkButton(self.pagination_frame, text="Next", width=100, command=self.go_next_page)
        self.next_btn.pack(side="left", padx=5)
        ctk.CTkLabel(self.pagination_frame, text="Page size:").pack(side="left", padx=(16, 5))
        self.page_size_dropdown = ctk.CTkComboBox(
            self.pagination_frame,
            values=[str(size) for size in PAGE_SIZES],
            variable=self.page_size_var,
            width=100,
            command=self.on_page_size_change,
        )
        self.page_size_dropdown.pack(side="left", padx=5)
        self.pagination_frame.pack_forget()

        self.show_labels()

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
            database="Neo4j",
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

    def update_summary(self, metadata=None, selected_summary=None):
        if metadata is not None:
            self.graph_metadata = metadata
        if selected_summary is not None:
            self.selected_database_summary = selected_summary
        metadata = self.graph_metadata
        self.summary_cards["Labels"].configure(text=str(metadata.get("labels", "--")))
        self.summary_cards["Total Nodes"].configure(text=str(metadata.get("nodes", "--")))
        self.summary_cards["Relationships"].configure(text=str(metadata.get("relationships", "--")))
        self.summary_cards["Selected"].configure(text=str(self.selected_database_summary))

    def show_labels(self):
        """Fetch and display label statistics (nodes + relations)."""
        start = time.perf_counter()
        records_returned = ""
        total_records = ""
        self.set_loading("Loading Neo4j labels...")
        try:
            for i in self.labels_tree.get_children():
                self.labels_tree.delete(i)
            self.label_counts = {}

            try:
                try:
                    metadata = self.backend.client.get_metadata()
                    self.update_summary(metadata)
                except Exception as e:
                    print("Erreur get_metadata:", e)
                    self.update_summary()

                labels = self.backend.client.list_databases()
                records_returned = len(labels)

                for lbl in labels:
                    label_name = lbl["name"]
                    node_count = lbl["count"]

                    try:
                        rel_count = self.backend.client.count_relationships(label_name)
                    except Exception:
                        rel_count = 0

                    self.label_counts[label_name] = node_count

                    self.labels_tree.insert(
                        "", "end",
                        values=(label_name, node_count, rel_count)
                    )
                total_records = sum(self.label_counts.values())

            except Exception as e:
                messagebox.showerror("Erreur", f"Impossible de charger les labels:\n{e}")
        finally:
            self.log_gui_metric(
                "scope_load",
                start,
                records_returned=records_returned,
                total_records=total_records,
            )
            self.clear_loading()


    def on_label_double_click(self, event):
        """When user double-clicks a label, show its nodes."""
        selected = self.labels_tree.focus()
        if not selected:
            return

        values = self.labels_tree.item(selected, "values")
        self.selected_label = values[0]
        self.selected_total_nodes = self.label_counts.get(self.selected_label, int(values[1]))
        selected_relationships = values[2] if len(values) > 2 else "--"
        self.total_records = self.selected_total_nodes
        self.current_page = 1
        self.current_page_size = int(self.page_size_var.get())
        self.filter_mode = None
        self.filter_value_entry.configure(state="normal")
        self.filter_value_entry.delete(0, "end")
        self.set_filter_mode_label(None)
        self.update_summary(
            selected_summary="{}/{}".format(self.selected_total_nodes, selected_relationships)
        )
        self.load_filter_properties()
        self.render_nodes_page()

        self.export_btn.configure(state="normal")
        self.graph_btn.configure(state="normal")

    def display_nodes(self, nodes):
        """Display nodes in treeview."""
        for col in self.nodes_tree["columns"]:
            self.nodes_tree.heading(col, text="")
        self.nodes_tree.delete(*self.nodes_tree.get_children())

        if not nodes:
            self.preview_label.configure(text="Filter result: 0 matching nodes")
            self.update_pagination_controls()
            return

        columns = sorted({k for n in nodes for k in n.keys()})
        self.nodes_tree["columns"] = columns

        for col in columns:
            self.nodes_tree.heading(col, text=col)
            self.nodes_tree.column(col, width=120, anchor="center")

        for n in nodes:
            self.nodes_tree.insert("", "end", values=[n.get(c, "") for c in columns])

        offset = (self.current_page - 1) * self.current_page_size
        start = offset + 1 if self.total_records else 0
        end = min(offset + len(nodes), self.total_records)
        self.preview_label.configure(
            text="Showing {}-{} of {} nodes".format(start, end, self.total_records)
        )

        self.update_pagination_controls()

    def load_filter_properties(self):
        try:
            self.label_properties = self.backend.client.get_label_properties(self.selected_label)
        except Exception as e:
            print("Erreur get_label_properties:", e)
            self.label_properties = []

        try:
            self.indexed_filter_properties = self.backend.client.get_indexed_filter_properties(self.selected_label)
        except Exception as e:
            print("Erreur get_indexed_filter_properties:", e)
            self.indexed_filter_properties = []

        self.filter_property_dropdown.configure(values=self.label_properties)
        if self.label_properties:
            self.filter_property_var.set(self.label_properties[0])
            state = "normal"
        else:
            self.filter_property_var.set("")
            state = "disabled"

        self.filter_property_dropdown.configure(state=state)
        self.filter_value_entry.configure(state=state)
        self.search_btn.configure(state=state)
        self.refresh_btn.configure(state=state)

    def apply_filter(self):
        if not self.selected_label:
            return

        property_name = self.filter_property_var.get().strip()
        value = self.filter_value_entry.get().strip()
        if not property_name or not value:
            messagebox.showinfo("Info", "Please select a property and enter a value")
            return

        self.current_page = 1
        if property_name in self.indexed_filter_properties:
            self.filter_mode = "indexed"
            self.set_filter_mode_label("indexed")
        else:
            self.filter_mode = "preview"
            self.set_filter_mode_label("preview")
        self.render_nodes_page()

    def refresh_current_view(self):
        self.filter_value_entry.delete(0, "end")
        self.filter_mode = None
        self.set_filter_mode_label(None)
        self.total_records = self.selected_total_nodes
        self.current_page = 1
        if self.selected_label:
            self.load_filter_properties()
            self.render_nodes_page()

    def set_filter_mode_label(self, mode):
        if mode == "indexed":
            self.filter_mode_label.configure(text="Indexed", text_color="#2E8B57")
        elif mode == "preview":
            self.filter_mode_label.configure(text="No index", text_color="#8A6A00")
        else:
            self.filter_mode_label.configure(text="", text_color="#1e1e1e")

    def render_nodes_page(self):
        start = time.perf_counter()
        nodes = []
        self.set_loading("Loading Neo4j nodes...")
        try:
            try:
                offset = (self.current_page - 1) * self.current_page_size
                if self.filter_mode == "indexed":
                    nodes, filtered_total, _fields = self.backend.client.search_indexed_nodes(
                        self.selected_label,
                        self.filter_property_var.get(),
                        self.filter_value_entry.get(),
                        offset=offset,
                        limit=self.current_page_size,
                    )
                    self.total_records = int(filtered_total)
                else:
                    self.total_records = self.selected_total_nodes
                    nodes = self.backend.client.list_documents(
                        None,
                        self.selected_label,
                        offset=offset,
                        limit=self.current_page_size,
                    )
                    if self.filter_mode == "preview":
                        nodes = self.filter_visible_nodes(nodes)
                self.display_nodes(nodes)
                if self.filter_mode == "preview":
                    self.preview_label.configure(
                        text="No index: {} matching visible nodes".format(len(nodes))
                    )
            except Exception as e:
                messagebox.showerror("Erreur", f"Erreur list_documents: {e}")
        finally:
            self.log_gui_metric(
                "first_page" if self.current_page == 1 else "next_page",
                start,
                page_size=self.current_page_size,
                page=self.current_page,
                records_returned=len(nodes),
                total_records=self.total_records,
            )
            self.clear_loading()

    def filter_visible_nodes(self, nodes):
        property_name = self.filter_property_var.get()
        values = self.search_value_variants(self.filter_value_entry.get())
        return [
            node for node in nodes
            if property_name in node and node.get(property_name) in values
        ]

    def search_value_variants(self, value):
        raw_value = str(value).strip()
        values = [raw_value]
        lowered = raw_value.lower()

        if lowered in ("true", "false"):
            values.append(lowered == "true")

        try:
            values.append(int(raw_value))
        except ValueError:
            pass

        try:
            values.append(float(raw_value))
        except ValueError:
            pass

        unique_values = []
        for item in values:
            if item not in unique_values:
                unique_values.append(item)
        return unique_values

    def update_pagination_controls(self):
        if not self.selected_label or self.total_records <= 0:
            self.pagination_frame.pack_forget()
            return

        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        self.page_indicator_label.configure(text="Page {} of {}".format(self.current_page, total_pages))
        self.prev_btn.configure(state="disabled" if self.current_page <= 1 else "normal")
        self.next_btn.configure(state="disabled" if self.current_page >= total_pages else "normal")
        self.pagination_frame.pack(side="right")

    def on_page_size_change(self, _value):
        start = time.perf_counter()
        self.current_page_size = int(self.page_size_var.get())
        if self.selected_label:
            self.current_page = 1
            self.render_nodes_page()
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
            self.render_nodes_page()

    def go_next_page(self):
        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        if self.current_page < total_pages:
            self.current_page += 1
            self.render_nodes_page()

    def export_label(self):
        if not self.selected_label:
            return
        try:
            offset = (self.current_page - 1) * self.current_page_size
            if self.filter_mode == "indexed":
                nodes, _total, _fields = self.backend.client.search_indexed_nodes(
                    self.selected_label,
                    self.filter_property_var.get(),
                    self.filter_value_entry.get(),
                    offset=offset,
                    limit=self.current_page_size,
                )
            else:
                nodes = self.backend.client.list_documents(
                    None,
                    self.selected_label,
                    offset=offset,
                    limit=self.current_page_size,
                )
                if self.filter_mode == "preview":
                    nodes = self.filter_visible_nodes(nodes)
            if not nodes:
                messagebox.showinfo("Info", "Aucune donnée à exporter.")
                return

            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                title="Enregistrer sous..."
            )
            if not file_path:
                return

            columns = sorted({k for n in nodes for k in n.keys()})
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(nodes)

            messagebox.showinfo("Export", f"Export réussi : {file_path}")
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur d’export: {e}")


    def show_graph(self):
        """Visualize label as graph with readable nodes and colors."""
        if not self.selected_label:
            return

        start = time.perf_counter()
        rel_count = ""
        try:
            rels = self.backend.client.list_relationships(
                self.selected_label,
                limit=NEO4J_GRAPH_PREVIEW_LIMIT,
            )
            rel_count = len(rels)
            if not rels:
                messagebox.showinfo("Info", "Aucune relation trouvée.")
                return

            G = nx.DiGraph()

            node_labels = {}
            node_colors = []
            color_map = {}

            palette = [
                "#66c2a5", "#fc8d62", "#8da0cb",
                "#e78ac3", "#a6d854", "#ffd92f"
            ]

            def get_color(label):
                if label not in color_map:
                    color_map[label] = palette[len(color_map) % len(palette)]
                return color_map[label]

            for record in rels:
                n = record["n"]
                m = record["m"]
                r = record["r"]

                # ---- Node labels ----
                n_label = list(n.labels)[0] if n.labels else "Node"
                m_label = list(m.labels)[0] if m.labels else "Node"

                n_name = n.get("name") or f"{n_label}_{n.id}"
                m_name = m.get("name") or f"{m_label}_{m.id}"

                # ---- Add nodes ----
                G.add_node(n_name, label=n_label)
                G.add_node(m_name, label=m_label)

                # ---- Add edge ----
                G.add_edge(n_name, m_name, label=r.type)

            # ---- Layout (stable) ----
            pos = nx.spring_layout(G, seed=42, k=2.2 , iterations=80)

            # ---- Node colors by label ----
            for node in G.nodes():
                label = G.nodes[node]["label"]
                node_colors.append(get_color(label))

            plt.figure(figsize=(9, 7))

            nx.draw(
                G,
                pos,
                with_labels=True,
                node_color=node_colors,
                node_size=1600,
                font_size=7,
                font_weight="bold",
                edgecolors="black"
            )

            # ---- Relationship labels ----
            edge_labels = nx.get_edge_attributes(G, "label")
            nx.draw_networkx_edge_labels(
                G,
                pos,
                edge_labels=edge_labels,
                font_size=6
            )

            # ---- Legend ----
            legend_handles = [
                plt.Line2D([0], [0], marker='o', color='w',
                        markerfacecolor=color, markersize=10, label=lbl)
                for lbl, color in color_map.items()
            ]
            plt.legend(handles=legend_handles, title="Node Labels")

            plt.title(f"Graph — {self.selected_label}")
            plt.axis("off")
            plt.tight_layout()
            plt.show()
            self.log_gui_metric(
                "graph_prepare",
                start,
                records_returned=rel_count,
                total_records=self.total_records,
            )

        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur show_graph: {e}")

