import sys
import os
import logging
from src.processor import AudienceProcessor

# Configuración global de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/execution.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("Main")

def main():
    logger.info("--- Iniciando Sistema de Generación de Audiencias ---")
    
    try:
        processor = AudienceProcessor()
        processor.process_audiences()
        logger.info("--- Ejecución completada exitosamente ---")
        
    except FileNotFoundError as e:
        logger.error(f"Error de archivo: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Error inesperado en la ejecución principal: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
