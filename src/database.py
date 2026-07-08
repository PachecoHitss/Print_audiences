import teradatasql
import logging
from src.config import get_config

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.creds = get_config().get_teradata_creds()
        self.connection = None

    def connect(self):
        try:
            logger.info(f"Intentando conectar a Teradata host: {self.creds['TD_HOST']}...")
            self.connection = teradatasql.connect(
                host=self.creds['TD_HOST'],
                user=self.creds['TD_USER'],
                password=self.creds['TD_PASSWORD']
            )
            logger.info("Conexión a Teradata exitosa.")
        except Exception as e:
            logger.error(f"Error conectando a Teradata: {e}")
            raise

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Conexión cerrada.")

    def execute_query(self, query, params=None):
        if not self.connection:
            self.connect()
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            description = cursor.description
            rows = cursor.fetchall()
            return description, rows
        except Exception as e:
            logger.error(f"Error ejecutando query: {e}")
            raise
        finally:
            cursor.close()


_db_manager = None


def get_db_manager() -> DatabaseManager:
    """Devuelve la instancia única de DatabaseManager (lazy initialization)."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager
