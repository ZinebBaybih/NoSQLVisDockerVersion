# app/views/neo4j_viewer.py
import customtkinter as ctk
from tkinter import ttk, messagebox
import csv
from tkinter import filedialog
import matplotlib.pyplot as plt
import networkx as nx

class Neo4jContentViewer(ctk.CTkFrame):
    """Enhanced Neo4j viewer with node & relationship statistics."""

    def __init__(self, master, backend, **kwargs):
        # super().__init__(master, **kwargs)
        super().__init__(master, fg_color="#ffffff", **kwargs)  # background color
        self.backend = backend
        self.selected_label = None

        self.pack(fill="both", expand=True, padx=10, pady=10)

        # ---------------- Header ----------------
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

        # ---------------- Action Buttons ----------------
        btn_frame = ctk.CTkFrame(self, fg_color="#ffffff")
        btn_frame.pack(fill="x", pady=10)

        self.export_btn = ctk.CTkButton(btn_frame, text="Export", command=self.export_label, state="disabled")
        self.export_btn.pack(side="left", padx=10)

        self.graph_btn = ctk.CTkButton(btn_frame, text="Graph", command=self.show_graph, state="disabled")
        self.graph_btn.pack(side="left", padx=10)

        self.back_btn = ctk.CTkButton(btn_frame, text="⬅ Back", command=self.go_back, state="disabled")
        self.back_btn.pack(side="left", padx=10)

        # ---------------- Node Details ----------------
        self.nodes_tree = ttk.Treeview(self, show="headings")
        self.nodes_tree.pack(fill="both", expand=True, pady=5)

        self.show_labels()

    # ============================================================
    # DISPLAY LABELS
    # ============================================================
    def show_labels(self):
        """Fetch and display label statistics (nodes + relations)."""
        for i in self.labels_tree.get_children():
            self.labels_tree.delete(i)

        try:
            labels = self.backend.client.list_databases()  # Labels
            for lbl in labels:
                label_name = lbl["name"]
                node_count = lbl["count"]
                try:
                    rels = self.backend.client.list_relationships(label_name)
                    rel_count = len(rels)
                except Exception:
                    rel_count = 0
                self.labels_tree.insert("", "end", values=(label_name, node_count, rel_count))
        except Exception as e:
            messagebox.showerror("Erreur", f"Impossible de charger les labels:\n{e}")

    # ============================================================
    # NODES
    # ============================================================
    def on_label_double_click(self, event):
        """When user double-clicks a label, show its nodes."""
        selected = self.labels_tree.focus()
        if not selected:
            return

        values = self.labels_tree.item(selected, "values")
        self.selected_label = values[0]

        try:
            nodes = self.backend.client.list_documents(None, self.selected_label)
            self.display_nodes(nodes)
        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur list_documents: {e}")
            return

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

    # ============================================================
    # EXPORT CSV
    # ============================================================
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

    # ============================================================
    # GRAPH VISUALIZATION
    # ============================================================
    def show_graph(self):
        """Visualize label as graph with relationship counts."""
        if not self.selected_label:
            return
        try:
            rels = self.backend.client.list_relationships(self.selected_label)
            if not rels:
                messagebox.showinfo("Info", "Aucune relation trouvée.")
                return

            G = nx.DiGraph()
            for record in rels:
                n = record["n"]
                m = record["m"]
                r = record["r"]
                G.add_edge(str(n.id), str(m.id), label=r.type)

            plt.figure(figsize=(7, 6))
            pos = nx.spring_layout(G, k=0.5, iterations=30)
            node_sizes = [400 + 50 * G.degree(n) for n in G.nodes()]
            node_colors = ["#66ccff" if G.degree(n) < 3 else "#ffaa00" for n in G.nodes()]

            nx.draw(
                G, pos, with_labels=True, node_size=node_sizes,
                node_color=node_colors, font_size=8, font_weight="bold"
            )
            nx.draw_networkx_edge_labels(G, pos, edge_labels=nx.get_edge_attributes(G, "label"))
            plt.title(f"Graph — Label: {self.selected_label} ({len(G.nodes())} nodes / {len(G.edges())} rels)")
            plt.show()

        except Exception as e:
            messagebox.showerror("Erreur", f"Erreur show_graph: {e}")

    # ============================================================
    # BACK
    # ============================================================
    def go_back(self):
        self.selected_label = None
        self.nodes_tree.delete(*self.nodes_tree.get_children())
        self.export_btn.configure(state="disabled")
        self.graph_btn.configure(state="disabled")
        self.back_btn.configure(state="disabled")
