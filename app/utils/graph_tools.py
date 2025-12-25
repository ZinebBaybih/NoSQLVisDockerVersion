# app/utils/graph_tools.py
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from collections import Counter

def plot_bar_pie(parent, labels, values, xlabel="", ylabel=""):
    if not labels:
        return

    popup = parent  # Assume parent = Toplevel
    fig, axs = plt.subplots(1,2, figsize=(10,4))
    fig.patch.set_facecolor("#e3e3e3")

    axs[0].bar(labels, values, color="#4fa3ff")
    axs[0].set_title("Barres", color="#1e1e1e")
    axs[0].set_xlabel(xlabel, color="#1e1e1e")
    axs[0].set_ylabel(ylabel, color="#1e1e1e")
    axs[0].tick_params(axis='x', rotation=45, colors="#1e1e1e")
    axs[0].tick_params(axis='y', colors="#1e1e1e")

    axs[1].pie(values, labels=labels, autopct="%1.1f%%")
    for ax in axs:
        ax.set_facecolor("#e3e3e3")
    fig.tight_layout()

    canvas = FigureCanvasTkAgg(fig, master=popup)
    canvas.draw()
    canvas.get_tk_widget().pack(fill="both", expand=True)
