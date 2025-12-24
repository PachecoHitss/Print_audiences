import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import logging
from src.database import db_manager

logger = logging.getLogger(__name__)

class AudienceProcessor:
    def __init__(self):
        # Rutas base (ajustar según entorno real si es necesario)
        self.input_path = Path(r"D:\LABORAL\Projects\Project_A\data\printAudiencesData\PrintAudiencesData.csv")
        self.output_base_path = Path(r"D:\OneDrive - Comunicacion Celular S.A.- Comcel S.A\Archivos de GCCO CG - Bases Certificadas\especificas")
        
    def read_input_file(self):
        """Lee el archivo CSV de entrada."""
        if not self.input_path.exists():
            raise FileNotFoundError(f"No se encuentra el archivo de entrada: {self.input_path}")
        
        try:
            # Asumimos separador ';' según el ejemplo
            df = pd.read_csv(self.input_path, sep=';', dtype=str)
            # Limpiar nombres de columnas (quitar espacios extra)
            df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            logger.error(f"Error leyendo archivo de entrada: {e}")
            raise

    def get_query_template(self, channel_suffix, channel_name=None):
        """Retorna el template SQL según el sufijo del canal y el nombre del canal."""
        
        norm_channel = str(channel_name).upper().strip() if channel_name else ""

        # 1. EMAIL (_EP)
        if channel_suffix == '_EP':
            return """
            Select DISTINCT A.EMAIL, trim(A.CUENTA) CUENTA, trim(A.CUENTA_H) CUENTA_H, A.TELE_NUMB, A.IDENTIFICACION , (B.NEMOTECNIA1 || B.NEMOTECNIA2) AS NEMOTECNIA
            from INNOVACION.SENT_CAMPAIGNS_LOG_EMAIL A
            LEFT JOIN INNOVACION.GROWTH_CAMPAIGN_OPTION B
            ON A.NEMOTECNIA = B.NEMOTECNIA 
            LEFT JOIN INNOVACION.INH_SEG_BSCS_CLIENTES C
            ON RIGHT(A.TELE_NUMB,10) = RIGHT(C.TELE_NUMB, 10)
            WHERE TRUNC (A.SENT_AT) = DATE '{execution_date}'
            AND A.NEMOTECNIA = '{full_nemotecnia}'
            AND A.CONTROL_GROUP = 'N'
            GROUP BY A.NEMOTECNIA, A.EMAIL, A.CUENTA, A.CUENTA_H, A.TELE_NUMB, A.IDENTIFICACION, B.NEMOTECNIA1, B.NEMOTECNIA2
            ORDER BY 1
            """
        
        # 2. IN_APP (_IP) O PUSH_SUPER_APP (Específico)
        # Solo PUSH_SUPER_APP lleva campos complementarios. NOTIFICATION_PUSH_COTA va por la general.
        elif channel_suffix == '_IP' or norm_channel == 'PUSH_SUPER_APP':
            return """
            Select 
            DISTINCT '57'||right(A.TELE_NUMB,10) TELE_NUMB
            , B.IDENTIFICACION 
            from INNOVACION.VW_SENT_CAMPAIGNS_LOG A
            LEFT JOIN INNOVACION.POTENCIAL_CAMPANA B ON RIGHT(A.TELE_NUMB, 10) = RIGHT(B.TELE_NUMB, 10)
            WHERE TRUNC (SENT_AT) = DATE '{execution_date}'
            AND A.NEMOTECNIA = '{full_nemotecnia}'
            and CONTROL_GROUP = 'N'
            ORDER BY 1
            """
            
        # 3. GENERAL (WPP, SMS, RCS, SAT_PUSH, NOTIFICATION_PUSH_COTA, etc.)
        else:
            return """
            Select DISTINCT A.TELE_NUMB
            from INNOVACION.VW_SENT_CAMPAIGNS_LOG A
            WHERE TRUNC (SENT_AT) = DATE '{execution_date}'
            and nemotecnia = '{full_nemotecnia}'
            and CONTROL_GROUP = 'N'
            ORDER BY 1
            """

    def format_date_for_sql(self, date_yymmdd):
        """Convierte YYMMDD a YYYY-MM-DD."""
        try:
            dt = datetime.strptime(date_yymmdd, "%y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Formato de fecha inválido: {date_yymmdd}")

    def _confirm_overwrite(self, file_path, new_count):
        """Solicita confirmación al usuario si el archivo ya existe."""
        if not file_path.exists():
            return True
        
        print(f"\n[ALERTA] El archivo ya existe: {file_path.name}")
        print(f"       El NUEVO archivo tendría {new_count} registros.")
        while True:
            response = input("¿Desea sobrescribirlo? (s/n): ").lower().strip()
            if response == 's':
                return True
            elif response == 'n':
                return False

    def get_channel_suffix(self, channel_name):
        """Mapea el nombre del canal a su sufijo correspondiente."""
        mapping = {
            'WHATSAPP': '_WPP',
            'SMS': '_SL',
            'SAT_PUSH': '_S0',
            'RCS': '_RCS',
            'NOTIFICATION_PUSH_COTA': '_AP', # Corregido según imagen (NOTIFICATION)
            'NOTIFICACION_PUSH_COTA': '_AP', # Mantengo variante en español por seguridad
            'PUSH_SUPER_APP': '_AP',
            'IN_APP': '_IP',
            'EMAIL': '_EP'
        }
        # Normalizar entrada: mayúsculas y quitar espacios
        clean_name = str(channel_name).upper().strip()
        return mapping.get(clean_name)

    def extract_base_nemotecnia(self, full_nemotecnia, suffix):
        """
        Elimina el sufijo del canal de la nemotecnia completa.
        Ej: C_INN_POS_QUINVPY_S0 con sufijo _S0 -> C_INN_POS_QUINVPY
        """
        if full_nemotecnia.endswith(suffix):
            return full_nemotecnia[:-len(suffix)]
        return full_nemotecnia

    def process_audiences(self):
        """Proceso principal."""
        logger.info("Iniciando procesamiento de audiencias...")
        
        try:
            df = self.read_input_file()
            
            db_manager.connect()

            for index, row in df.iterrows():
                try:
                    # Extraer datos del CSV
                    # EXECUTION_DATE;MESSAGE_TYPE;SEGMENT;CHANNEL;NEMOTECNIA;RECORDS
                    exec_date_raw = row['EXECUTION_DATE'] # YYMMDD
                    channel_name = row['CHANNEL']         # SMS, WHATSAPP, etc.
                    full_nemotecnia = row['NEMOTECNIA']   # C_INN_POS_QUINVPY_S0
                    
                    # Determinar carpeta de salida basada en la fecha de ejecución del registro
                    # Convertir YYMMDD (251226) a YYYYMMDD (20251226)
                    try:
                        dt_exec = datetime.strptime(exec_date_raw, "%y%m%d")
                        folder_name = dt_exec.strftime("%Y%m%d")
                    except ValueError:
                        logger.error(f"Fecha inválida en fila {index}: {exec_date_raw}. Saltando.")
                        continue

                    output_dir = self.output_base_path / folder_name
                    # Crear carpeta si no existe (idempotente)
                    output_dir.mkdir(parents=True, exist_ok=True)

                    # Obtener sufijo
                    channel_suffix = self.get_channel_suffix(channel_name)
                    if not channel_suffix:
                        logger.warning(f"Canal desconocido '{channel_name}' en fila {index}. Saltando.")
                        continue

                    # Extraer nemotecnia base para el nombre del archivo
                    nemotecnia_base = self.extract_base_nemotecnia(full_nemotecnia, channel_suffix)
                    
                    sql_date = self.format_date_for_sql(exec_date_raw)
                    
                    # Nombre de archivo de salida
                    # NEMOTECNIA_BASE + EXECUTION_DATE + CHANNEL_SUFFIX
                    filename = f"{nemotecnia_base}{exec_date_raw}{channel_suffix}.txt"
                    file_path = output_dir / filename
                    logger.info(f"Procesando: {full_nemotecnia} para fecha {sql_date} en carpeta {folder_name}")
                    
                    # Obtener Query
                    query_template = self.get_query_template(channel_suffix, channel_name)
                    query = query_template.format(
                        execution_date=sql_date,
                        full_nemotecnia=full_nemotecnia
                    )
                    
                    # Ejecutar Query
                    cursor = db_manager.execute_query(query)
                    
                    # Obtener encabezados (nombres de columnas)
                    headers = [desc[0] for desc in cursor.description] if cursor.description else []
                    
                    rows = cursor.fetchall()
                    count = len(rows)
                    logger.info(f"Registros encontrados: {count}")

                    # Validar cantidad mínima de registros (>= 50)
                    if count < 50:
                        logger.warning(f"Registros insuficientes ({count}) para {full_nemotecnia}. Mínimo requerido: 50. Archivo NO generado.")
                        continue

                    # Validar existencia (solo si cumple el mínimo)
                    if not self._confirm_overwrite(file_path, count):
                        logger.info(f"Archivo omitido por el usuario: {filename}")
                        continue

                    # Escribir archivo
                    # Separador '|' si hay más de una columna
                    with open(file_path, 'w', encoding='utf-8') as f:
                        # Escribir encabezados
                        if headers:
                            f.write("|".join(headers) + "\n")
                            
                        for row_data in rows:
                            # Convertir todos los elementos a string y unir con pipe
                            line = "|".join([str(item) if item is not None else '' for item in row_data])
                            f.write(line + "\n")
                            
                    logger.info(f"Archivo generado: {filename} ({len(rows)} registros)")

                except Exception as e:
                    logger.error(f"Error procesando fila {index}: {e}")
                    # Continuar con la siguiente fila a pesar del error
                    continue

        except Exception as e:
            logger.critical(f"Error crítico en el proceso: {e}")
        finally:
            db_manager.disconnect()
            logger.info("Proceso finalizado.")

if __name__ == "__main__":
    # Configuración de log para ejecución directa
    logging.basicConfig(level=logging.INFO)
    processor = AudienceProcessor()
    processor.process_audiences()
