from sqlmodel import create_engine
from dotenv import load_dotenv
import os


class Paths:
    load_dotenv()
    ENGINE = create_engine(os.getenv("ENGINE_PATH"))
    
    TRAFFIC_USERNAME: str = os.getenv(r"TRAFFIC_USERNAME")
    TRAFFIC_PASSWORD: str = os.getenv(r"TRAFFIC_PASSWORD")
    URL_LOGIN_TRAFFIC: str = os.getenv(r"URL_LOGIN_TRAFFIC")
    URL_DATA_TRAFFIC: str = os.getenv(r"URL_DATA_TRAFFIC")
    URL_DATA_TRAFFIC_PRESUPUESTO: str = os.getenv(r"URL_DATA_TRAFFIC_PRESUPUESTO")
    
    URL_LOGIN_ODDO: str = os.getenv(r"URL_LOGIN_ODDO")
    URL_DATA_ODDO: str = os.getenv(r"URL_DATA_ODDO")
    ODDO_USERNAME: str = os.getenv(r"ODDO_USERNAME")
    ODDO_PASSWORD: str = os.getenv(r"ODDO_PASSWORD")


