# app/main.py
import customtkinter as ctk
from tkinter import ttk
import threading
import traceback
from controllers.connection_controller import ConnectionController

ctk.set_appearance_mode("white")
ctk.set_default_color_theme("blue")

class NoSQLVisualizerApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("NoSQL Visualizer")
        self.geometry("900x600")

        # Tabs
        self.tab_control = ttk.Notebook(self)
        self.tab_conn = ctk.CTkFrame(self.tab_control)
        self.tab_vis = ctk.CTkFrame(self.tab_control)
        self.tab_logout = ctk.CTkFrame(self.tab_control)
        self.tab_control.add(self.tab_conn, text="Connection")
        self.tab_control.add(self.tab_vis, text="Visualization")
        self.tab_control.add(self.tab_logout, text="Logout")
        self.tab_control.pack(expand=True, fill="both")

        # Controller
        self.controller = ConnectionController(self.tab_conn, self.tab_vis, self.tab_control, self.tab_logout)

if __name__ == "__main__":
    app = NoSQLVisualizerApp()
    app.mainloop()
