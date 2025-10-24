import pandas as pd
from bs4 import BeautifulSoup
import time, logging, requests, datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from Pipeline.utils import Paths

# from utils import Paths


class Scraper:
    @staticmethod
    def scrape_oddo() -> pd.DataFrame:
        def get_tables(data):
            rows = []
            for tbody in data:
                for row in tbody.find_all("tr", class_="o_data_row"):
                    row_data = {}

                    # Extrae las celdas usando name=...
                    def safe_get(td_name, attr=None, default=""):
                        cell = row.find("td", {"name": td_name})
                        if not cell:
                            return default
                        if attr:
                            return cell.get(attr, default)
                        return cell.get_text(strip=True)

                    row_data["name"] = safe_get("name")
                    row_data["email_from"] = safe_get("email_from", attr="data-tooltip")
                    row_data["user_id"] = safe_get("user_id", attr="data-tooltip")
                    row_data["expected_revenue"] = (
                        row.find("td", {"name": "expected_revenue"})
                        .find("span")
                        .get_text(strip=True)
                        if row.find("td", {"name": "expected_revenue"})
                        and row.find("td", {"name": "expected_revenue"}).find("span")
                        else "U$D 0,00"
                    )
                    row_data["stage_id"] = safe_get("stage_id")

                    rows.append(row_data)
            return pd.DataFrame(rows)

        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")

        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 20)

        # === LOGIN ===
        driver.get(Paths.URL_LOGIN_ODDO)
        time.sleep(2)
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='login']"))
        ).send_keys(Paths.ODDO_USERNAME)

        wait.until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='password']"))
        ).send_keys(Paths.ODDO_PASSWORD)

        wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[@id='wrapwrap']//form//button"))
        ).click()
        time.sleep(2)
        # === NAVEGAR AL MÓDULO DE DATOS ===
        driver.get(Paths.URL_DATA_ODDO)
        time.sleep(10)
        # Cerrar modal de flujo si existe (a veces Odoo no lo muestra)
        try:
            button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button.o_facet_remove"))
            )
            button.click()
            print("Ventana de flujo cerrada.")
        except Exception as e:
            print(f"No se encontró ventana de flujo, continuando... ({e})")

        time.sleep(10)
        # === SCRAPER PRINCIPAL ===
        df_list = []

        while True:
            try:
                wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "o_pager_value"))
                )

                html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")

                current_range = soup.find(class_="o_pager_value").text
                total = soup.find(class_="o_pager_limit").text

                n = current_range.split("-")[-1].strip()
                print(f"Página actual: {current_range} / {total}")

                data = soup.find_all("tbody", class_="ui-sortable")
                df = get_tables(data)
                df_list.append(df)

                if n == total:
                    print("Última página alcanzada.")
                    break

                next_button = wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".o_pager_next"))
                )
                if "disabled" in next_button.get_attribute("class"):
                    print("Botón siguiente deshabilitado. Fin.")
                    break

                next_button.click()
                time.sleep(1.8)
            except Exception as e:
                print("No se pudo avanzar más:", e)
                driver.quit()
                break

        if not df_list:
            print("⚠️ No se extrajo ningún dato.")
            return pd.DataFrame()
        driver.quit()
        return pd.concat(df_list, ignore_index=True)

    @staticmethod
    def get_reserva(
        fecha_desde: datetime.date,
        fecha_hasta: datetime.date,
        cookies: dict,
        url_data: str = Paths.URL_DATA_TRAFFIC,
    ) -> pd.DataFrame:
        session = requests.Session()
        # Agregar cookies a la sesión
        for name, value in cookies.items():
            session.cookies.set(name, value, domain="traffic.welcomelatinamerica.com")

        all_data = []
        skip = 0
        # Cabeceras
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://traffic.welcomelatinamerica.com",
            "Referer": url_data,
            "User-Agent": "Mozilla/5.0",
        }
        while True:
            payload = {
                "Take": 2500,
                "Skip": skip,
                "Criteria": [
                    [["Fec_sal"], ">=", fecha_desde.strftime("%Y-%m-%d")],
                    "and",
                    [["Fec_sal"], "<", fecha_hasta.strftime("%Y-%m-%d")],
                ],
                "IncludeColumns": [
                    "Idreserva",
                    "Tiporva",
                    "Fec_mod",
                    "Fec_rva",
                    "Fec_sal",
                    "Fec_fin",
                    "Rva",
                    "Nombreagencia_cod_agcia",
                    "Estado",
                    "Can_adu",
                    "Can_chd",
                    "Descripparame_moneda",
                    "Nombregrupo",
                    "Nombrevendedor_cod_vdor",
                    "Total",
                    "gananciaTotal",
                    "Tipocont",
                    "Descripparame_productos",
                ],
            }

            response = session.post(
                url_data, headers=headers, cookies=cookies, json=payload
            )

            if response.status_code != 200:
                logging.debug(f"Error, status: {response.status_code}")
                print("Error:", response.status_code)
                break

            data = response.json().get("Entities", [])
            if not data:
                break  # No quedan más filas

            all_data.extend(data)
            skip += 2500
            print(f"Traído {len(data)} filas, total acumulado: {len(all_data)}")

            columnas = [
                "Idreserva",
                "Rva",
                "Tiporva",
                "Fec_mod",
                "Fec_rva",
                "Fec_sal",
                "Fec_fin",
                "Nombreagencia_cod_agcia",  # cliente
                "Nombrevendedor_cod_vdor",
                "Nombregrupo",
                "Estado",
                "Can_adu",
                "Can_chd",
                "Moneda",
                "Total",
                "gananciaTotal",
                "Tipocont",
                "Descripparame_productos",
            ]

            return pd.DataFrame(all_data, columns=columnas)

    @staticmethod
    def get_presupesto(
        fecha_desde: datetime.date,
        fecha_hasta: datetime.date,
        cookies: dict,
        url_data: str = Paths.URL_DATA_TRAFFIC_PRESUPUESTO,
    ) -> pd.DataFrame:
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": "https://traffic.welcomelatinamerica.com",
            "Referer": url_data,
            "User-Agent": "Mozilla/5.0",
        }

        session = requests.Session()
        # Agregar cookies a la sesión
        for name, value in cookies.items():
            session.cookies.set(name, value, domain="traffic.welcomelatinamerica.com")

        all_data = []
        skip = 0

        while True:
            payload = {
                "Take": 2500,
                "Skip": skip,
                "Criteria": [
                    [["Fec_sal"], ">=", fecha_desde.strftime("%Y-%m-%d")],
                    "and",
                    [["Fec_sal"], "<", fecha_hasta.strftime("%Y-%m-%d")],
                ],
                "IncludeColumns": [
                    "Idpresupu",
                    "Rva",
                    "Tiporva",
                    "Fec_mod",
                    "Fec_rva",
                    "Fec_sal",
                    "Nombreagencia_cod_agcia",
                    "Observ",
                    "Estado",
                    "Can_adu",
                    "Can_chd",
                    "Moneda",
                    "Nombrevendedor_cod_vdor",
                    "Total",
                    "costoConIva",
                    "GananciaTotal",
                    "Productos",
                ],
            }

            response = session.post(
                url_data, headers=headers, cookies=cookies, json=payload
            )

            if response.status_code != 200:
                logging.debug(f"Error, status: {response.status_code}")
                print("Error:", response.status_code)
                print(
                    "Detalle del servidor:", response.text[:1000]
                )  # muestra los primeros 1000 caracteres
                break

            data = response.json().get("Entities", [])
            if not data:
                break  # No quedan más filas

            all_data.extend(data)
            skip += 2500
            print(f"Traído {len(data)} filas, total acumulado: {len(all_data)}")

        cols: list = [
            "Idpresupu",
            "Rva",
            "Tiporva",
            "Fec_mod",
            "Fec_rva",
            "Fec_sal",
            "Nombreagencia_cod_agcia",
            "Observ",
            "Estado",
            "Can_adu",
            "Can_chd",
            "Moneda",
            "Nombrevendedor_cod_vdor",
            "Total",
            "costoConIva",
            "GananciaTotal",
            "Productos",
        ]
        return pd.DataFrame(all_data, columns=cols)

    @staticmethod
    def scrape_all(
        fecha_desde: datetime.date,
        fecha_hasta: datetime.date,
        logger: logging.Logger,
    ):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")  # maximiza ventana
        options.add_argument("--headless")  # modo headless
        options.add_argument("--disable-gpu")  # recomendado en headless
        options.add_argument("--window-size=1920,1080")  # tamaño de la ventana"""

        driver = webdriver.Chrome(options=options)

        driver.get(Paths.URL_LOGIN_TRAFFIC)
        logger.info("=" * 60)
        logger.info("Abriendo página de login de traffic...")
        wait = WebDriverWait(driver, 15)
        wait.until(
            EC.presence_of_element_located(
                (By.ID, "Softur_Serene_Membership_LoginPanel0_Username")
            )
        ).send_keys(Paths.TRAFFIC_USERNAME)

        wait.until(
            EC.presence_of_element_located(
                (By.ID, "Softur_Serene_Membership_LoginPanel0_Password")
            )
        ).send_keys(Paths.TRAFFIC_PASSWORD)

        wait.until(
            EC.element_to_be_clickable(
                (By.ID, "Softur_Serene_Membership_LoginPanel0_LoginButton")
            )
        ).click()
        # Cookies obtenidas de Selenium
        wait.until(EC.url_changes(Paths.URL_LOGIN_TRAFFIC))

        logger.info("Ingreso.")

        selenium_cookies = driver.get_cookies()
        cookies = {c["name"]: c["value"] for c in selenium_cookies}

        reservas: pd.DataFrame = Scraper.get_reserva(fecha_desde, fecha_hasta, cookies)
        logger.info("Reserevas terminado. Esperando presupuestos...")
        presupuestos: pd.DataFrame = Scraper.get_presupesto(
            fecha_desde, fecha_hasta, cookies
        )
        driver.quit()
        logger.info("Presupuesto terminado. Esperando oddo...")
        oddo: pd.DataFrame = Scraper.scrape_oddo()
        logger.info("Oddo terminado. Proceso de scraping finalizado.")
        logger.info("=" * 60)
        return reservas, presupuestos, oddo
