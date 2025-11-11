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


def filter_data1(df: pd.DataFrame) -> pd.DataFrame:
    tipo_rva_list: list = [
        "PURE TRAVEL GROUP",
        "DIRECTO TSA INCOMING",
        "B2B FIT TSA INCOMING",
        "B2B GRP TSA INCOMING",
        "FEEL SOUTHAMERICA",
        "Opcionales OPNS de TRIPS",
        "OPCIONALES BUENOS AIRES",
        "OPCIONAL FEEL SOUTHAMERICA",
    ]

    return df.loc[
        (df["tip_serv"] == "HO")
        & (df["estadoRva"].isin(["OP", "CO", "CP"]))
        & (df["estado"].isin(["OK", "PC", "PP", "SP"]))
        & (df["Tiporva"].isin(tipo_rva_list))
    ]


def filter_data2(df: pd.DataFrame) -> pd.DataFrame:
    tipo_rva_list: list = [
        "PURE TRAVEL GROUP",
        "DIRECTO TSA INCOMING",
        "B2B FIT TSA INCOMING",
        "B2B GRP TSA INCOMING",
        "FEEL SOUTHAMERICA",
        "DIRECTO TSA VIAJES",
        "B2B FIT TSA VIAJES",
        "B2B GRP TSA VIAJES",
        "OPCIONALES OPNS DE TRIPS",
        "OPCIONALES BUENOS AIRES",
        "OPCIONAL FEEL SOUTHAMERICA",
    ]
    return df.loc[
        (df["tip_serv"] == "HO")
        & (df["estadoRva"].isin(["OP", "CO", "CP"]))
        & (df["estado"].isin(["OK", "PC", "PP", "SP"]))
        & (df["Tiporva"].isin(tipo_rva_list))
    ]


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

    def filter_data1(df: pd.DataFrame) -> pd.DataFrame:
        tipo_rva_list: list = [
            "PURE TRAVEL GROUP",
            "DIRECTO TSA INCOMING",
            "B2B FIT TSA INCOMING",
            "B2B GRP TSA INCOMING",
            "FEEL SOUTHAMERICA",
            "OPCIONALES OPNS DE TRIPS",
            "OPCIONALES BUENOS AIRES",
            "OPCIONAL FEEL SOUTHAMERICA",
        ]

        return df.loc[
            (df["tip_serv"] == "HO")
            & (df["estadoRva"].isin(["OP", "CO", "CP"]))
            & (df["estado"].isin(["OK", "PC", "PP", "SP"]))
            & (df["Tiporva"].isin(tipo_rva_list))
        ]

    def filter_data2(df: pd.DataFrame) -> pd.DataFrame:
        tipo_rva_list: list = [
            "PURE TRAVEL GROUP",
            "DIRECTO TSA INCOMING",
            "B2B FIT TSA INCOMING",
            "B2B GRP TSA INCOMING",
            "FEEL SOUTHAMERICA",
            "DIRECTO TSA VIAJES",
            "B2B FIT TSA VIAJES",
            "B2B GRP TSA VIAJES",
            "OPCIONALES OPNS DE TRIPS",
            "OPCIONALES BUENOS AIRES",
            "OPCIONAL FEEL SOUTHAMERICA",
        ]
        return df.loc[
            (df["tip_serv"] == "HO")
            & (df["estadoRva"].isin(["OP", "CO", "CP"]))
            & (df["estado"].isin(["OK", "PC", "PP", "SP"]))
            & (df["Tiporva"].isin(tipo_rva_list))
        ]

    def filter_data3(df: pd.DataFrame) -> pd.DataFrame:
        tipo_rva_list: list = [
            "B2B GRP TSA INCOMING",
        ]
        return df.loc[
            (df["tip_serv"] == "HO")
            & (df["estadoRva"].isin(["DB", "CX", "BL", "XC"]))
            & (df["estado"].isin(["DB"]))
            & (df["Tiporva"].isin(tipo_rva_list))
        ]

    def filter_data4(df: pd.DataFrame) -> pd.DataFrame:
        tipo_rva_list: list = [
            "FEEL SOUTHAMERICA",
            "B2B FIT TSA INCOMING",
        ]
        return df.loc[
            (df["tip_serv"] == "HO")
            & (df["estadoRva"].isin(["DB", "CX", "BL", "XC", "OP", "CP", "CO"]))
            & (df["estado"].isin(["DB", "CX"]))
            & (df["Tiporva"].isin(tipo_rva_list))
        ]

    data["tip_serv"] = data["tip_serv"].str.upper().str.strip()
    data["estado"] = data["estado"].str.upper().str.strip()
    data["estadoRva"] = data["estadoRva"].str.upper().str.strip()
    data["Tiporva"] = data["Tiporva"].str.upper().str.strip()
    date_list: list = [
        "FechaViaje",
        "fecha_in",
        "fecha_out",
    ]
    for i in date_list:
        data[i] = pd.to_datetime(data[i], errors="coerce").dt.date

    data1: pd.DataFrame = filter_data1(data)
    data2: pd.DataFrame = filter_data2(data)
    data3: pd.DataFrame = filter_data3(data)
    data4: pd.DataFrame = filter_data4(data)

    rdo1 = (
        data1.groupby(["nombre_proveedor", "nom_oper"])["vts_habitacion"]
        .sum()
        .to_frame()
        .reset_index()
    )
    rdo2 = (
        data2.groupby(["nombre_proveedor", "nom_oper"])["vts_habitacion"]
        .sum()
        .to_frame()
        .reset_index()
    )
    iatas: str = "CiudadesList_20251107_151611.xlsx"
    df_iatas = pd.read_excel(iatas)

    destino = data1.groupby("destino")["vts_habitacion"].sum().to_frame().reset_index()
    resultado = destino.merge(
        df_iatas[["Codigociudad", "Nombreciudad"]],
        left_on="destino",  # En df1
        right_on="Codigociudad",  # En df_iatas
        how="left",
    )
    resultado.drop(columns=["Codigociudad"], inplace=True)

    df1 = filter_data3(data3)
    df1.groupby(["nombre_proveedor", "nom_oper"])[
        "vts_habitacion"
    ].sum().to_frame().reset_index()

    df2 = filter_data4(data4)
    df2.groupby(["nombre_proveedor", "nom_oper"])[
        "vts_habitacion"
    ].sum().to_frame().reset_index()

    nombre_archivo = "mis_datos.xlsx"

    # Usar ExcelWriter para escribir en múltiples hojas
    with pd.ExcelWriter(nombre_archivo) as writer:
        # Escribir el primer DataFrame en la hoja 'Hoja1'
        rdo1.to_excel(writer, sheet_name="SIN TSA Viajes", index=False)
        rdo2.to_excel(writer, sheet_name="CON TSA Viajes", index=False)
        resultado.to_excel(writer, sheet_name="Destino", index=False)
        df1.to_excel(writer, sheet_name="RN Grupos Desbloqueadas", index=False)
        df2.to_excel(writer, sheet_name="RN FIT Canceladas", index=False)
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
