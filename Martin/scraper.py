import pandas as pd
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime, logging, requests, time


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # También muestra en consola
        ],
    )

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("INICIANDO PROCESO DE DESCARGA")
    logger.info("=" * 60)
    return logger


def main(
    FECHA_IN_DESDE: str,
    FECHA_IN_HASTA: str,
    FECHA_VIAJE_DESDE: str,
    FECHA_VIAJE_HASTA: str,
):
    logger = setup_logging()
    logger.info("Iniciando scraper de costo reserva...")
    load_dotenv()  # This will load variables from .env

    URL_LOGIN_TRAFFIC: str = os.getenv("URL_LOGIN_TRAFFIC")
    TRAFFIC_USERNAME: str = os.getenv("TRAFFIC_USERNAME")
    TRAFFIC_PASSWORD: str = os.getenv("TRAFFIC_PASSWORD")
    URL_DATA_TRAFFIC_COSTORESERVA: str = os.getenv("URL_DATA_TRAFFIC_COSTORESERVA")

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")  # maximiza ventana
    options.add_argument("--headless")  # modo headless
    options.add_argument("--disable-gpu")  # recomendado en headless
    options.add_argument("--window-size=1920,1080")  # tamaño de la ventana"""

    driver = webdriver.Chrome(options=options)

    driver.get(URL_LOGIN_TRAFFIC)
    logger.info("=" * 60)
    logger.info("Abriendo página de login de traffic...")
    wait = WebDriverWait(driver, 15)
    wait.until(
        EC.presence_of_element_located(
            (By.ID, "Softur_Serene_Membership_LoginPanel0_Username")
        )
    ).send_keys(TRAFFIC_USERNAME)

    wait.until(
        EC.presence_of_element_located(
            (By.ID, "Softur_Serene_Membership_LoginPanel0_Password")
        )
    ).send_keys(TRAFFIC_PASSWORD)

    wait.until(
        EC.element_to_be_clickable(
            (By.ID, "Softur_Serene_Membership_LoginPanel0_LoginButton")
        )
    ).click()
    # Cookies obtenidas de Selenium
    wait.until(EC.url_changes(URL_LOGIN_TRAFFIC))

    logger.info("Ingreso.")

    FECHA_RESERVA_INICIO: str = datetime.date(1990, 1, 1).strftime("%Y-%m-%d")
    FECHA_RESERVA_HOY: str = datetime.datetime.now().date().strftime("%Y-%m-%d")

    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://traffic.welcomelatinamerica.com",
        "Referer": "https://traffic.welcomelatinamerica.com/iTraffic_TSA/Z_Reportes/CostoReserva_List",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    }
    selenium_cookies = driver.get_cookies()
    cookies = {c["name"]: c["value"] for c in selenium_cookies}

    all_data: list = []
    skip = 0
    while True:
        try:
            payload = {
                "Take": 2500,
                "Skip": skip,
                "fec_rvadesde": FECHA_RESERVA_INICIO,
                "fec_rvahasta": FECHA_RESERVA_HOY,
                "fec_Saldesde": FECHA_VIAJE_DESDE,
                "fec_Salhasta": FECHA_VIAJE_HASTA,
                "fec_Indesde": FECHA_IN_DESDE,
                "fec_Inhasta": FECHA_IN_HASTA,
            }

            response = requests.post(
                URL_DATA_TRAFFIC_COSTORESERVA,
                headers=headers,
                cookies=cookies,
                json=payload,
            )
            if response.status_code != 200:
                logger.error(
                    f"Error en la solicitud: {response.status_code} - {response.text}"
                )
                return pd.DataFrame()
            data_batch: dict = response.json().get("Entities", [])
            if not data_batch:
                logger.info("No hay más datos para descargar. Fin del scraping.")
                break
            all_data.extend(data_batch)
            skip += 2500
            logger.info(f"Registros descargados hasta ahora: {len(all_data)}")
        finally:
            driver.quit()
    df: pd.DataFrame = pd.DataFrame(all_data)
    logger.info(f"Datos obtenidos: {len(df)} registros.")
    logger.info("Finalizando scraper de costo reserva.")
    logger.info("Filtrando y renombrando columnas...")
    data: pd.DataFrame = df.filter(
        [
            "idOrden",
            "destino",
            "tip_serv",
            "Rva",
            "FechaViaje",
            "ApePaxTitular",
            "Can_adu",
            "can_chd",
            "can_inf",
            "servicio",
            "in_",
            "out_",
            "estado",
            "estadoRva",
            "cant_hab",
            "nts",
            "nts_habitacion",
            "nomprov",
            "nom_oper",
            "NombreCliente",
            "Tiporva",
            "nom_vdor",
            "costo",
            "ivacosto",
            "venta",
            "ivaventa",
        ],
        axis=1,
    )
    data.rename(
        columns={
            "in_": "fecha_in",
            "out_": "fecha_out",
            "nts_habitacion": "vts_habitacion",
            "nomprov": "nombre_proveedor",
        },
        inplace=True,
    )
    logger.info("Columnas filtradas y renombradas.")
    logger.info("=" * 60)
    return data
