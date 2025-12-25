# app/controllers/connection_controller.py
import threading
import traceback
import customtkinter as ctk
from models.backend import NoSQLBackend
from views.mongo_viewer import MongoContentViewer
from views.redis_viewer import RedisContentViewer
from views.neo4j_viewer import Neo4jContentViewer
from views.cassandra_viewer import CassandraContentViewer
from views.logout_viewer import LogoutContentViewer

class ConnectionController:
    def __init__(self, conn_tab, vis_tab, tab_control, logout_tab):
        self.conn_tab = conn_tab
        self.vis_tab = vis_tab
        self.tab_control = tab_control
        self.logout_tab = logout_tab

        self.backend = None
        self.viewer = None

        self.default_ports = {
            "Redis": "6379",
            "Cassandra": "9042",
            "Neo4j": "7687",
            "MongoDB": "27017"
        }


        self.build_ui()

    def build_ui(self):
        title = ctk.CTkLabel(self.conn_tab, text="NoSQL Database Connection",
                            font=ctk.CTkFont(size=18, weight="bold"))
        title.pack(pady=15)


        self.db_type_var = ctk.StringVar()
        self.db_dropdown = ctk.CTkComboBox(self.conn_tab, variable=self.db_type_var, values=["MongoDB", "Redis", "Neo4j", "Cassandra"], width=200)
        self.db_dropdown.set("MongoDB")
        self.db_dropdown.pack(pady=10)
        self.db_type_var.trace("w", self.on_db_type_change)


        self.status_label = ctk.CTkLabel(self.conn_tab, text="Status: Not connected", fg_color="orange")
        self.status_label.pack(pady=8, fill="x")

        self.host_entry = ctk.CTkEntry(self.conn_tab, placeholder_text="Host (default: localhost)")
        self.host_entry.pack(pady=5, padx=20, fill="x")
        self.host_entry.insert(0, "localhost")

        self.port_entry = ctk.CTkEntry(self.conn_tab, placeholder_text="Port (e.g., 27017, 6379, 7687)")
        self.port_entry.pack(pady=5, padx=20, fill="x")
        self.port_entry.insert(0, "27017")

        self.user_entry = ctk.CTkEntry(self.conn_tab, placeholder_text="User (optional)")
        self.user_entry.pack(pady=5, padx=20, fill="x")

        self.pwd_entry = ctk.CTkEntry(self.conn_tab, placeholder_text="Password (optional)", show="*")
        self.pwd_entry.pack(pady=5, padx=20, fill="x")

        self.conn_btn = ctk.CTkButton(self.conn_tab, text="Connect", fg_color="red", command=self.toggle_connection)
        self.conn_btn.pack(pady=15)

        self.tab_control.tab(self.vis_tab, state="disabled")
        self.tab_control.tab(self.logout_tab, state="disabled")

    def toggle_connection(self):
        if self.backend:
            try:
                self.backend.disconnect()
            except:
                pass
            self.backend = None
            # Reset connection status
            self.status_label.configure(text="Status: Disconnected", fg_color="orange")
            self.conn_btn.configure(text="Connect", fg_color="red")

            # Enable Connection tab, disable others
            self.tab_control.tab(self.conn_tab, state="normal")
            self.tab_control.tab(self.vis_tab, state="disabled")
            self.tab_control.tab(self.logout_tab, state="disabled")

            # Clear any widgets from Visualization and Logout tabs
            for w in self.vis_tab.winfo_children():
                w.destroy()
            for w in self.logout_tab.winfo_children():
                w.destroy()

            # Redirect back to Connection tab
            self.tab_control.select(self.conn_tab)
            return
        
        

        threading.Thread(target=self._connect, daemon=True).start()


    def _connect(self):
        db_type = self.db_dropdown.get()
        host = self.host_entry.get() or "localhost"
        port = int(self.port_entry.get()) if self.port_entry.get() else 27017
        user = self.user_entry.get() or None
        pwd = self.pwd_entry.get() or None

        # Validation
        if db_type == "Neo4j" and (not user or not pwd):
            self.status_label.configure(text="Neo4j requires user and password!", fg_color="red")
            return
        self.status_label.configure(text="Connecting...", fg_color="orange")
        self.conn_btn.configure(text="Connecting...", fg_color="orange")


        # Backend connection can run in thread
        try:
            backend = NoSQLBackend(db_type=db_type, host=host, port=port, user=user, password=pwd)
            backend.connect()
        except Exception as e:
            self.conn_tab.after(0, lambda: self.status_label.configure(text=f"Error: {e}", fg_color="red"))
            return

        # Once connected, **schedule all GUI updates on the main thread**
        self.conn_tab.after(0, lambda: self._on_connected(db_type, host, port, user, backend))

    def _on_connected(self, db_type, host, port, user, backend):
        self.backend = backend
        self.status_label.configure(text=f"Connected to {db_type} at {host}:{port}", fg_color="green")
        self.conn_btn.configure(text="Disconnect", fg_color="green")

        # Enable/Disable tabs and Redirect user to Visualization tab
        self.tab_control.tab(self.vis_tab, state="normal")
        self.tab_control.tab(self.logout_tab, state="normal")
        self.tab_control.tab(self.conn_tab, state="disabled")
        self.tab_control.select(self.vis_tab)

        # Clear previous widgets
        for w in self.vis_tab.winfo_children():
            w.destroy()
        for w in self.logout_tab.winfo_children():
            w.destroy()

        # Load the viewer (GUI, must be on main thread!)
        if db_type == "MongoDB":
            self.viewer = MongoContentViewer(self.vis_tab, self.backend)
        elif db_type == "Redis":
            self.viewer = RedisContentViewer(self.vis_tab, self.backend)
        elif db_type == "Neo4j":
            self.viewer = Neo4jContentViewer(self.vis_tab, self.backend)
        elif db_type == "Cassandra":
            self.viewer = CassandraContentViewer(self.vis_tab, self.backend)

        # Load Logout viewer
        self.logout_viewer = LogoutContentViewer(self.logout_tab,  controller=self, user=user, db_type=db_type, host=host, port=port)



    
    def on_db_type_change(self, *args):
        db = self.db_type_var.get()
        if db in self.default_ports:
            self.port_entry.delete(0, "end")
            self.port_entry.insert(0, self.default_ports[db])
        # Clear username & password fields
        self.user_entry.delete(0, "end")
        self.pwd_entry.delete(0, "end")

