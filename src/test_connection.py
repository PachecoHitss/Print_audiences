import sys
import os

# Agregar el directorio raíz al path para poder importar src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import db_manager
import logging

# Configurar logger para ver output en consola
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestConnection")

def test_connection():
    print("--- Iniciando prueba de conexión a Teradata ---")
    try:
        # Intentar conectar
        db_manager.connect()
        
        # Ejecutar una query simple para validar
        print("Ejecutando query de prueba (SELECT DATE)...")
        cursor = db_manager.execute_query("SELECT DATE")
        result = cursor.fetchone()
        
        print(f"¡Conexión Exitosa! Fecha del servidor: {result[0]}")
        
    except Exception as e:
        print("\nXXX Falló la conexión XXX")
        print(f"Error: {e}")
        print("\nSugerencia: Si el error es de autenticación, verifica si necesitas 'logmech=LDAP' o 'TD2' en src/database.py")
    finally:
        db_manager.disconnect()
        print("--- Fin de la prueba ---")

if __name__ == "__main__":
    test_connection()
