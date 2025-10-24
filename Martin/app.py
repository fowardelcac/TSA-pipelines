import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from tkcalendar import Calendar
import sys
import pandas as pd
from scraper import main as scraper_main
from scraper import setup_logging
import datetime


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
    logger = setup_logging()
    fecha_in_desde: str = cal_in_desde.get_date()
    fecha_in_hasta: str = cal_in_hasta.get_date()
    # Fechas viaje
    fecha_viaje_desde: str = cal_viaje_desde.get_date()
    fecha_viaje_hasta: str = cal_viaje_hasta.get_date()
    # Validaciones
    if fecha_in_desde >= fecha_in_hasta:
        messagebox.showerror(
            "Error", "ETL: La fecha 'Hasta' debe ser posterior a 'Desde'."
        )
        return
    if fecha_viaje_desde >= fecha_viaje_hasta:
        messagebox.showerror(
            "Error", "Viaje: La fecha 'Hasta' debe ser posterior a 'Desde'."
        )
        return

    status_label.config(
        text=f"Ejecutando ETL desde {fecha_in_desde} hasta {fecha_in_hasta} "
        f"y viaje desde {fecha_viaje_desde} hasta {fecha_viaje_hasta}..."
    )

    data: pd.DataFrame = scraper_main(
        fecha_in_desde, fecha_in_hasta, fecha_viaje_desde, fecha_viaje_hasta
    )
    data_ho = data.loc[data["tip_serv"] == "HO"]
    rdo = (
        data_ho.groupby("nombre_proveedor")["vts_habitacion"]
        .sum()
        .to_frame()
        .reset_index()
    )
    nombre_archivo = "mis_datos.xlsx"

    # Usar ExcelWriter para escribir en múltiples hojas
    with pd.ExcelWriter(nombre_archivo) as writer:
        # Escribir el primer DataFrame en la hoja 'Hoja1'
        rdo.to_excel(writer, sheet_name="Agrupacion", index=False)

        data_ho.to_excel(writer, sheet_name="Filtrado_HO", index=False)
        data.to_excel(writer, sheet_name="Completo", index=False)

    status_label.config(
        text=f"✅ ETL-Traffic finalizado ({fecha_in_desde} → {fecha_in_hasta}, "
        f"{fecha_viaje_desde} → {fecha_viaje_hasta})"
    )

    logger.info(
        f"Datos exportados a C:\\Users\\jsaldano\\Documents\\Martin\\{nombre_archivo}"
    )
    logger.info("ETL finalizado.")
    logger.info("=" * 60)


# -------------------------------------------------------
# Ventana principal
# -------------------------------------------------------
root = tk.Tk()
root.title("Pipeline - TSA trips")
root.geometry("1000x1000")

# --- Selector de fecha ---
frame_date = ttk.Frame(root)
frame_date.pack(pady=15)

tk.Label(
    frame_date,
    text="Seleccione el rango de fechas para ETL-Traffic:",
    font=("Arial", 18, "bold"),
).pack(pady=5)

# --- Calendarios originales (rango ETL) ---
frame_cals_top = ttk.Frame(frame_date)
frame_cals_top.pack(pady=10)

# Calendario "Desde" ETL
frame_desde = ttk.Frame(frame_cals_top)
frame_desde.pack(side="left", padx=20)
tk.Label(frame_desde, text="FECHA_IN_DESDE:", font=("Arial", 10, "bold")).pack()
cal_in_desde = Calendar(frame_desde, selectmode="day", date_pattern="yyyy-mm-dd")
cal_in_desde.pack(pady=5)
# Calendario "Hasta" ETL
frame_hasta = ttk.Frame(frame_cals_top)
frame_hasta.pack(side="left", padx=20)
tk.Label(frame_hasta, text="FECHA_IN_HASTA:", font=("Arial", 10, "bold")).pack()
cal_in_hasta = Calendar(frame_hasta, selectmode="day", date_pattern="yyyy-mm-dd")
cal_in_hasta.pack(pady=5)

# --- Segunda fila: Calendarios "fecha_viaje" ---
frame_cals_bottom = ttk.Frame(frame_date)
frame_cals_bottom.pack(pady=10)

# Calendario "Desde" viaje
frame_viaje_desde = ttk.Frame(frame_cals_bottom)
frame_viaje_desde.pack(side="left", padx=20)
tk.Label(
    frame_viaje_desde, text="FECHA_VIAJE_DESDE:", font=("Arial", 10, "bold")
).pack()
cal_viaje_desde = Calendar(
    frame_viaje_desde, selectmode="day", date_pattern="yyyy-mm-dd"
)
cal_viaje_desde.pack(pady=5)

# Calendario "Hasta" viaje
frame_viaje_hasta = ttk.Frame(frame_cals_bottom)
frame_viaje_hasta.pack(side="left", padx=20)
tk.Label(
    frame_viaje_hasta, text="FECHA_VIAJE_HASTA:", font=("Arial", 10, "bold")
).pack()
cal_viaje_hasta = Calendar(
    frame_viaje_hasta, selectmode="day", date_pattern="yyyy-mm-dd"
)
cal_viaje_hasta.pack(pady=5)
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
# --- Logs debajo del botón ---
log_text = scrolledtext.ScrolledText(
    frame_buttons, wrap=tk.WORD, height=20, state="normal"
)
log_text.pack(fill="both", expand=True, padx=10, pady=10)

# Redirigir stdout/stderr
sys.stdout = ConsoleRedirect(log_text)
sys.stderr = ConsoleRedirect(log_text)

# Agregar la pestaña al Notebook
notebook.add(frame_buttons, text="Acciones")
# Iniciar la aplicación
root.mainloop()
