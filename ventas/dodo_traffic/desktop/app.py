import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from tkcalendar import Calendar
import sys, os
import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from Pipeline.etl import main_etl
from Pipeline.functions import Loader
from Pipeline.model import Vendedor
from sqlmodel import Session
from Pipeline.utils import Paths  # donde está tu ENGINE

# -------------------------------------------------------
# Redirección de consola
# -------------------------------------------------------
class ConsoleRedirect:
    """Redirige stdout/stderr a un widget Text de tkinter"""

    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, msg):
        self.text_widget.insert(tk.END, msg)
        self.text_widget.see(tk.END)  # autoscroll

    def flush(self):
        pass


# -------------------------------------------------------
# Función para correr ETL
# -------------------------------------------------------
def run_traffic():
    selected_date = cal_desde.get_date()
    selected_date1 = cal_hasta.get_date()

    date_obj = datetime.datetime.strptime(selected_date, "%m/%d/%y").date()
    date_obj1 = datetime.datetime.strptime(selected_date1, "%m/%d/%y").date()

    if date_obj >= date_obj1:
        messagebox.showerror(
            "Error en las fechas",
            "La fecha 'Hasta' debe ser posterior a la fecha 'Desde'.",
        )
        return

    status_label.config(
        text=f"Ejecutando ETL desde {date_obj.isoformat()} hasta {date_obj1.isoformat()}..."
    )

    main_etl(date_obj, date_obj1)

    status_label.config(
        text=f"✅ ETL-Traffic finalizado ({date_obj.isoformat()} → {date_obj1.isoformat()})"
    )


# -------------------------------------------------------
# Función para crear vendedor
# -------------------------------------------------------


def on_create_vendor():
    nombre_completo = entry_nombre_completo.get().strip().upper()
    nombre = entry_nombre.get().strip().upper()

    if not nombre_completo or not nombre:
        messagebox.showwarning("Campos vacíos", "Debe completar ambos campos.")
        return

    try:
        with Session(Paths.ENGINE) as session:
            Loader.create_vendor(nombre_completo, nombre, session)
            session.commit()
        messagebox.showinfo("Éxito", f"Vendedor '{nombre_completo}' agregado correctamente.")
        entry_nombre_completo.delete(0, tk.END)
        entry_nombre.delete(0, tk.END)
    except Exception as e:
        messagebox.showerror("Error al insertar", str(e))


# -------------------------------------------------------
# Ventana principal
# -------------------------------------------------------
root = tk.Tk()
root.title("Pipeline - TSA trips")
root.geometry("850x600")

# --- Selector de fecha ---
frame_date = ttk.Frame(root)
frame_date.pack(pady=15)

tk.Label(
    frame_date,
    text="Seleccione el rango de fechas para ETL-Traffic:",
    font=("Arial", 11, "bold"),
).pack(pady=5)

frame_cals = ttk.Frame(frame_date)
frame_cals.pack(pady=10)

# Calendario "Desde"
frame_desde = ttk.Frame(frame_cals)
frame_desde.pack(side="left", padx=20)
tk.Label(frame_desde, text="Desde:", font=("Arial", 10, "bold")).pack()
cal_desde = Calendar(frame_desde, selectmode="day")
cal_desde.pack(pady=5)

# Calendario "Hasta"
frame_hasta = ttk.Frame(frame_cals)
frame_hasta.pack(side="left", padx=20)
tk.Label(frame_hasta, text="Hasta:", font=("Arial", 10, "bold")).pack()
cal_hasta = Calendar(frame_hasta, selectmode="day")
cal_hasta.pack(pady=5)

# --- Notebook principal ---
notebook = ttk.Notebook(root)
notebook.pack(fill="both", expand=True)

# Pestaña 1: Acciones ETL
frame_buttons = ttk.Frame(notebook)
btn1 = tk.Button(frame_buttons, text="Ejecutar ETL-Traffic", command=run_traffic)
btn1.pack(pady=10)
status_label = tk.Label(frame_buttons, text="Esperando acción...")
status_label.pack(pady=10)
notebook.add(frame_buttons, text="Acciones")

# Pestaña 2: Consola
frame_logs = ttk.Frame(notebook)
log_text = scrolledtext.ScrolledText(frame_logs, wrap=tk.WORD, state="normal")
log_text.pack(fill="both", expand=True)
notebook.add(frame_logs, text="Logs")

# Pestaña 3: Agregar vendedor
frame_vendedor = ttk.Frame(notebook)
ttk.Label(frame_vendedor, text="Nombre completo:", font=("Arial", 10, "bold")).pack(pady=5)
entry_nombre_completo = ttk.Entry(frame_vendedor, width=40)
entry_nombre_completo.pack(pady=5)

ttk.Label(frame_vendedor, text="Nombre corto:", font=("Arial", 10, "bold")).pack(pady=5)
entry_nombre = ttk.Entry(frame_vendedor, width=40)
entry_nombre.pack(pady=5)

btn_add_vendor = ttk.Button(frame_vendedor, text="Agregar vendedor", command=on_create_vendor)
btn_add_vendor.pack(pady=15)

notebook.add(frame_vendedor, text="Agregar vendedor")

# Redirigir stdout/stderr
sys.stdout = ConsoleRedirect(log_text)
sys.stderr = ConsoleRedirect(log_text)

root.mainloop()
