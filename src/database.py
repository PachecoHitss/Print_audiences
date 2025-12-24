import teradatasql
import logging
from src.config import config_loader

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.creds = config_loader.get_teradata_creds()
        self.connection = None

    def connect(self):
        """Establece la conexión con Teradata."""
        try:
            logger.info(f"Intentando conectar a Teradata host: {self.creds['TD_HOST']}...")
            self.connection = teradatasql.connect(
                host=self.creds['TD_HOST'],
                user=self.creds['TD_USER'],
                password=self.creds['TD_PASSWORD'],
                logmech='LDAP'  # Asumo LDAP por ser corporativo, si falla probaremos sin esto o con TD2
            )
            logger.info("Conexión a Teradata exitosa.")
        except Exception as e:
            logger.error(f"Error conectando a Teradata: {e}")
            raise

    def disconnect(self):
        """Cierra la conexión."""
        if self.connection:
            self.connection.close()
            logger.info("Conexión cerrada.")

    def execute_query(self, query, params=None):
        """Ejecuta una consulta y retorna un cursor."""
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            logger.error(f"Error ejecutando query: {e}")
            raise

db_manager = DatabaseManager()
