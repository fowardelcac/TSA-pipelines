import logging
from datetime import datetime


##########################################################################################
# RELACIONADO CON LOGS
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


class ProcessTracker:
    """Clase para trackear el proceso y generar reportes"""

    def __init__(self):
        self.stats = {
            "nuevos": 0,
            "actualizados": 0,
            "errores": 0,
            "total_procesadas": 0,
        }
        self.updated_records = []
        self.error_records = []
        self.new_records = []
        self.start_time = datetime.now()

    def add_new(self, reserva):
        """Registra un nuevo registro"""
        self.stats["nuevos"] += 1
        self.new_records.append(
            {
                "reserva": reserva,
                "accion": "NUEVO",
                "timestamp": datetime.now(),
            }
        )

    def add_update(self, reserva, changed_fields):
        """Registra una actualización"""
        self.stats["actualizados"] += 1
        self.updated_records.append(
            {
                "reserva": reserva,
                "accion": "ACTUALIZADO",
                "timestamp": datetime.now(),
                "detalle": f'Campos actualizados: {", ".join(changed_fields)}',
            }
        )
        print(self.updated_records[-1])

    def add_error(self, reserva, error_msg):
        """Registra un error"""
        self.stats["errores"] += 1
        self.error_records.append(
            {
                "reserva": reserva,
                "accion": "ERROR",
                "timestamp": datetime.now(),
                "detalle": f"Error durante procesamiento: {error_msg}",
            }
        )

    def increment_processed(self):
        """Incrementa el contador de filas procesadas"""
        self.stats["total_procesadas"] += 1

    def get_summary(self):
        """Retorna un resumen del proceso"""
        end_time = datetime.now()
        duration = end_time - self.start_time

        return {
            "inicio": self.start_time,
            "fin": end_time,
            "duracion": str(duration),
            "stats": self.stats,
        }
