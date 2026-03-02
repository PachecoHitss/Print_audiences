import sys
import os
import logging
import argparse
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
    parser = argparse.ArgumentParser(description="Sistema de Generación de Audiencias")
    parser.add_argument("--force", action="store_true", help="Sobrescribir archivos existentes automáticamente")
    parser.add_argument("--auto", action="store_true", help="Ejecución automática (no interactiva). Salta archivos existentes si no se usa --force")
    args = parser.parse_args()

    logger.info("--- Iniciando Sistema de Generación de Audiencias ---")
    
    try:
        # Si se usa force, asumimos modo automático también (no preguntar)
        is_interactive = not (args.auto or args.force)
        
        processor = AudienceProcessor(interactive=is_interactive, force_overwrite=args.force)
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
