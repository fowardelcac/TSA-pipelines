from Pipeline.functions import ProcessData, setup_logging, Loader
from Pipeline.scraper import Scraper
from Pipeline.utils import Paths
import datetime
from sqlmodel import Session


def main_etl(desde: datetime.date, hasta: datetime.date) -> None:
    logger = setup_logging()

    reservas, presupuestos, oddo = Scraper.scrape_all(desde, hasta, logger)

    reservas_f = ProcessData.process_rva(reservas)
    presupuestos_f = ProcessData.process_pres(presupuestos)
    oddo_f = ProcessData.process_oddo(oddo)

    with Session(Paths.ENGINE) as session:
        Loader.upsert_reservas(reservas_f, session, logger)
        Loader.upsert_presupuestos(presupuestos_f, session, logger)
        Loader.upsert_oddo(oddo_f, session, logger)


