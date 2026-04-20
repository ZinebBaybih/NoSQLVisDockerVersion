# app/views/neo4j_viewer.py
import customtkinter as ctk
from tkinter import ttk, messagebox
import csv
from tkinter import filedialog
import matplotlib.pyplot as plt
import networkx as nx

from config import PAGE_SIZES, PREVIEW_LIMIT

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

        self.pack(fill="both", expand=True, padx=10, pady=10)

        header = ctk.CTkLabel(self, text="Neo4j Content Viewer", font=("Arial", 22, "bold"), fg_color="#ffffff", text_color="#191919")
        header.pack(pady=10)

        # ---------------- Label Section ----------------
        label_frame = ctk.CTkFrame(self, corner_radius=10, fg_color="#ffffff")
        label_frame.pack(fill="x", pady=10)

        ctk.CTkLabel(label_frame, text="Available Databases", font=("Arial", 16, "bold"), text_color="#191919").pack(
            side="left", padx=10, pady=5
        )

        self.refresh_btn = ctk.CTkButton(
            label_frame, text="Refresh", command=self.show_labels, width=100
        )
        self.refresh_btn.pack(side="right", padx=10, pady=5)

        # Table for labels
        self.labels_tree = ttk.Treeview(
            self,
            columns=("label", "nodes", "rels"),
            show="headings",
            height=8
        )
        self.labels_tree.heading("label", text="Label")
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

        self.back_btn = ctk.CTkButton(btn_frame, text="⬅ Back", command=self.go_back, state="disabled")
        self.back_btn.pack(side="left", padx=10)

        self.page_size_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        self.page_size_frame.pack(fill="x", pady=(0, 6))
        ctk.CTkLabel(self.page_size_frame, text="Page size:").pack(side="left", padx=10)
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

        self.preview_label = ctk.CTkLabel(self, text="", anchor="w", font=("Arial", 12))
        self.preview_label.pack(fill="x", pady=(0, 4))

        self.graph_note_label = ctk.CTkLabel(
            self,
            text="Graph preview - first {} nodes shown".format(PREVIEW_LIMIT),
            anchor="w",
            font=("Arial", 12)
        )
        self.graph_note_label.pack(fill="x", pady=(0, 6))

        # ---------------- Node Details ----------------
        self.nodes_tree = ttk.Treeview(self, show="headings")
        self.nodes_tree.pack(fill="both", expand=True, pady=5)

        self.pagination_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        self.prev_btn = ctk.CTkButton(self.pagination_frame, text="Previous", width=100, command=self.go_prev_page)
        self.prev_btn.pack(side="left", padx=5)
        self.page_indicator_label = ctk.CTkLabel(self.pagination_frame, text="")
        self.page_indicator_label.pack(side="left", padx=10)
        self.next_btn = ctk.CTkButton(self.pagination_frame, text="Next", width=100, command=self.go_next_page)
        self.next_btn.pack(side="left", padx=5)
        self.pagination_frame.pack_forget()

        self.show_labels()


    def show_labels(self):
        """Fetch and display label statistics (nodes + relations)."""
        for i in self.labels_tree.get_children():
            self.labels_tree.delete(i)
        self.label_counts = {}

        try:
            labels = self.backend.client.list_databases()

            for lbl in labels:
                label_name = lbl["name"]
                node_count = lbl["count"]

                try:
                    rels = self.backend.client.list_relationships(label_name)
                    rel_count = len(rels)
                except Exception:
                    rel_count = 0

                self.label_counts[label_name] = node_count

                self.labels_tree.insert(
                    "", "end",
                    values=(label_name, node_count, rel_count)
                )

        except Exception as e:
            messagebox.showerror("Erreur", f"Impossible de charger les labels:\n{e}")


    def on_label_double_click(self, event):
        """When user double-clicks a label, show its nodes."""
        selected = self.labels_tree.focus()
        if not selected:
            return

        values = self.labels_tree.item(selected, "values")
        self.selected_label = values[0]
        self.selected_total_nodes = self.label_counts.get(self.selected_label, int(values[1]))
        self.total_records = self.selected_total_nodes
        self.current_page = 1
        self.current_page_size = int(self.page_size_var.get())
        self.page_size_frame.pack(fill="x", pady=(0, 6))

        self.render_nodes_page()

        self.export_btn.configure(state="normal")
        self.graph_btn.configure(state="normal")
        self.back_btn.configure(state="normal")

    def display_nodes(self, nodes):
        """Display nodes in treeview."""
        for col in self.nodes_tree["columns"]:
            self.nodes_tree.heading(col, text="")
        self.nodes_tree.delete(*self.nodes_tree.get_children())

        if not nodes:
            messagebox.showinfo("Info", "Aucun nœud trouvé pour ce label.")
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
            text="Showing {}-{} of {} nodes".format(start, end, self.selected_total_nodes)
        )

        self.update_pagination_controls()

    def render_nodes_page(self):
        try:
            nodes = self.backend.client.list_documents(
                None,
                self.selected_label,
                offset=(self.current_page - 1) * self.current_page_size,
                limit=self.current_page_size,
            )
            self.display_nodes(nodes)
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur list_documents: {e}")

    def update_pagination_controls(self):
        if not self.selected_label or self.total_records <= 0:
            self.pagination_frame.pack_forget()
            return

        total_pages = max(1, (self.total_records + self.current_page_size - 1) // self.current_page_size)
        self.page_indicator_label.configure(text="Page {} of {}".format(self.current_page, total_pages))
        self.prev_btn.configure(state="disabled" if self.current_page <= 1 else "normal")
        self.next_btn.configure(state="disabled" if self.current_page >= total_pages else "normal")
        self.pagination_frame.pack(fill="x", pady=(0, 5))

    def on_page_size_change(self, _value):
        self.current_page_size = int(self.page_size_var.get())
        if self.selected_label:
            self.current_page = 1
            self.render_nodes_page()

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
            nodes = self.backend.client.list_documents(None, self.selected_label)
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

        try:
            rels = self.backend.client.list_relationships(self.selected_label)
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

        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur show_graph: {e}")


    def go_back(self):
        self.selected_label = None
        self.selected_total_nodes = 0
        self.total_records = 0
        self.current_page = 1
        self.nodes_tree.delete(*self.nodes_tree.get_children())
        self.preview_label.configure(text="")
        self.page_size_frame.pack_forget()
        self.pagination_frame.pack_forget()
        self.export_btn.configure(state="disabled")
        self.graph_btn.configure(state="disabled")
        self.back_btn.configure(state="disabled")
