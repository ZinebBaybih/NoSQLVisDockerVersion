# app/views/logout_viewer.py
import customtkinter as ctk
from datetime import datetime
import os
from PIL import Image


class LogoutContentViewer(ctk.CTkFrame):
    """Logout tab with session info and logout button"""
    def __init__(self, master, controller, user=None, db_type=None, host=None, port=None):
        super().__init__(master, fg_color="#d4d4d4") 
        self.pack(fill="both", expand=True, padx=15, pady=15)
        self.controller = controller
        self.user = user or "Unknown"
        self.db_type = db_type or "Unknown"
        self.host = host or "localhost"
        self.port = port or "N/A"
        self.connected_at = datetime.now()  # record session start time

        self.build_ui()
    

    def build_ui(self):
        # Title
        ctk.CTkLabel(
            self,
            text="Session Information",
            font=ctk.CTkFont(size=18, weight="bold"),
            text_color="#0B0B0B"
        ).pack(pady=(15, 15))

        # ----- MAIN TWO-COLUMN AREA -----
        main_frame = ctk.CTkFrame(self, fg_color="#d4d4d4")
        main_frame.pack(fill="x", pady=(0, 20),padx=(80, 90))

        main_frame.grid_columnconfigure(0, weight=1)
        main_frame.grid_columnconfigure(1, weight=0)

        # LEFT SIDE – INFO FRAME
        info_frame = ctk.CTkFrame(main_frame,  fg_color="#d4d4d4")
        info_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 20))

        ctk.CTkLabel(info_frame, text=f"User: {self.user}", anchor="w", text_color="#000000").pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(info_frame, text=f"Database: {self.db_type}", anchor="w", text_color="#000000").pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(info_frame, text=f"Host: {self.host}", anchor="w", text_color="#000000").pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(info_frame, text=f"Port: {self.port}", anchor="w", text_color="#000000").pack(fill="x", padx=10, pady=5)
        ctk.CTkLabel(info_frame, text=f"Session Start: {self.connected_at.strftime('%d.%m.%Y %H:%M:%S')}", anchor="w", text_color="#000000").pack(fill="x", padx=10, pady=5)

        # RIGHT SIDE – IMAGE
        # Build absolute path to assets/logout.png
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        image_path = os.path.join(base_dir, "assets", "logout.png")

        try:
            self.side_image = ctk.CTkImage(
                light_image=Image.open(image_path),
                dark_image=Image.open(image_path),
                size=(200, 200)
            )

            ctk.CTkLabel(main_frame, image=self.side_image, text="").grid(row=0, column=1, sticky="e")

        except Exception as e:
            print("Image load error:", e)
            print("Tried path:", image_path)


        # ----- LOGOUT BUTTON -----
        logout_btn = ctk.CTkButton(
            self,
            text="Logout",
            fg_color="red",
            hover_color="#FF5555",
            command=self.logout
        )
        logout_btn.pack(pady=20)


    def logout(self):
        """Trigger the controller to disconnect"""
        if self.controller and hasattr(self.controller, "toggle_connection"):
            self.controller.toggle_connection()
