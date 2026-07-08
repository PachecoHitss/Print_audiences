import re
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from src.database import get_db_manager
from src.config import get_config

logger = logging.getLogger(__name__)

# Whitelist para prevenir SQL injection: solo alfanumérico y guiones bajos
_NEMOTECNIA_RE = re.compile(r'^[A-Za-z0-9_]+$')


class AudienceProcessor:
    def __init__(self, interactive=True, force_overwrite=False):
        paths = get_config().get_paths()
        self.input_path = Path(paths['input_file'])
        self.output_base_path = Path(paths['output_base'])
        self.servicio_base_path = Path(paths['servicio_base'])
        settings = get_config().get_settings()
        self.min_records = int(settings.get('min_records', 50))
        self.interactive = interactive
        self.force_overwrite = force_overwrite

    def read_input_file(self):
        if not self.input_path.exists():
            raise FileNotFoundError(f"No se encuentra el archivo de entrada: {self.input_path}")
        try:
            df = pd.read_csv(self.input_path, sep=';', dtype=str)
            df.columns = df.columns.str.strip()
            return df
        except Exception as e:
            logger.error(f"Error leyendo archivo de entrada: {e}")
            raise

    def get_query_template(self, channel_name):
        norm_channel = str(channel_name).upper().strip()
        if norm_channel == 'EMAIL':
            return """
            Select A.EMAIL, trim(A.CUENTA) CUENTA, trim(A.CUENTA_H) CUENTA_H, A.TELE_NUMB, A.IDENTIFICACION, (B.NEMOTECNIA1 || B.NEMOTECNIA2) AS NEMOTECNIA
            from INNOVACION.SENT_CAMPAIGNS_LOG_EMAIL A
            LEFT JOIN INNOVACION.GROWTH_CAMPAIGN_OPTION B ON A.NEMOTECNIA = B.NEMOTECNIA
            LEFT JOIN INNOVACION.INH_SEG_BSCS_CLIENTES C ON RIGHT(A.TELE_NUMB,10) = RIGHT(C.TELE_NUMB, 10)
            WHERE TRUNC(A.SENT_AT) = DATE '{execution_date}'
            AND A.NEMOTECNIA = '{full_nemotecnia}'
            AND A.CONTROL_GROUP = 'N'
            GROUP BY A.NEMOTECNIA, A.EMAIL, A.CUENTA, A.CUENTA_H, A.TELE_NUMB, A.IDENTIFICACION, B.NEMOTECNIA1, B.NEMOTECNIA2
            ORDER BY 1
            """
        elif norm_channel in ('IN_APP', 'PUSH_SUPER_APP'):
            return """
            Select DISTINCT '57'||right(A.TELE_NUMB,10) TELE_NUMB, B.IDENTIFICACION
            from INNOVACION.VW_SENT_CAMPAIGNS_LOG A
            LEFT JOIN INNOVACION.POTENCIAL_CAMPANA B ON RIGHT(A.TELE_NUMB, 10) = RIGHT(B.TELE_NUMB, 10)
            WHERE TRUNC(SENT_AT) = DATE '{execution_date}'
            AND A.NEMOTECNIA = '{full_nemotecnia}'
            and CONTROL_GROUP = 'N'
            ORDER BY 1
            """
        else:
            return """
            Select DISTINCT A.TELE_NUMB
            from INNOVACION.VW_SENT_CAMPAIGNS_LOG A
            WHERE TRUNC(SENT_AT) = DATE '{execution_date}'
            and nemotecnia = '{full_nemotecnia}'
            and CONTROL_GROUP = 'N'
            ORDER BY 1
            """

    def format_date_for_sql(self, date_yymmdd):
        try:
            dt = datetime.strptime(date_yymmdd, "%y%m%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Formato de fecha inválido: {date_yymmdd}")

    def _validate_sql_params(self, exec_date_raw, full_nemotecnia):
        """Valida los parámetros contra una whitelist para prevenir SQL injection."""
        if not re.match(r'^\d{6}$', exec_date_raw):
            raise ValueError(f"Fecha fuera de formato permitido (YYMMDD): {exec_date_raw!r}")
        if not _NEMOTECNIA_RE.match(full_nemotecnia):
            raise ValueError(f"Nemotecnia contiene caracteres no permitidos: {full_nemotecnia!r}")

    def _confirm_overwrite(self, file_path, new_count):
        if not file_path.exists():
            return True
        if self.force_overwrite:
            logger.info(f"Sobrescribiendo archivo existente por configuración: {file_path.name}")
            return True
        if not self.interactive:
            logger.info(f"Omitiendo archivo existente en modo no interactivo: {file_path.name}")
            return False
        print(f"\n[ALERTA] El archivo ya existe: {file_path.name}")
        print(f"       El NUEVO archivo tendría {new_count} registros.")
        while True:
            response = input("¿Desea sobrescribirlo? (s/n): ").lower().strip()
            if response == 's':
                return True
            elif response == 'n':
                return False

    def get_channel_suffix(self, channel_name):
        mapping = {
            'WHATSAPP': '_WPP',
            'SMS': '_SL',
            'SAT_PUSH': '_S0',
            'RCS': '_RCS',
            'NOTIFICATION_PUSH_COTA': '_AP',
            'NOTIFICACION_PUSH_COTA': '_AP',
            'PUSH_SUPER_APP': '_AP',
            'IN_APP': '_IP',
            'EMAIL': '_EP'
        }
        clean_name = str(channel_name).upper().strip()
        return mapping.get(clean_name)

    def extract_base_nemotecnia(self, full_nemotecnia, suffix):
        if full_nemotecnia.endswith(suffix):
            return full_nemotecnia[:-len(suffix)]
        return full_nemotecnia

    def process_audiences(self):
        logger.info("Iniciando procesamiento de audiencias...")
        db = get_db_manager()
        try:
            df = self.read_input_file()
            required_columns = {'EXECUTION_DATE', 'CHANNEL', 'NEMOTECNIA'}
            missing_cols = required_columns - set(df.columns)
            if missing_cols:
                raise ValueError(f"Columnas faltantes en el CSV de entrada: {missing_cols}")
            db.connect()
            for index, row in df.iterrows():
                try:
                    exec_date_raw = str(row['EXECUTION_DATE']).strip()
                    channel_name = str(row['CHANNEL']).strip()
                    full_nemotecnia = str(row['NEMOTECNIA']).strip()
                    try:
                        dt_exec = datetime.strptime(exec_date_raw, "%y%m%d")
                        folder_name = dt_exec.strftime("%Y%m%d")
                    except ValueError:
                        logger.error(f"Fecha inválida en fila {index}: {exec_date_raw}. Saltando.")
                        continue
                    channel_suffix = self.get_channel_suffix(channel_name)
                    if not channel_suffix:
                        logger.warning(f"Canal desconocido '{channel_name}' en fila {index}. Saltando.")
                        continue
                    try:
                        self._validate_sql_params(exec_date_raw, full_nemotecnia)
                    except ValueError as e:
                        logger.error(f"Parámetro inválido en fila {index}: {e}. Saltando.")
                        continue
                    if full_nemotecnia.startswith("N_"):
                        output_dir = self.servicio_base_path / folder_name
                    else:
                        output_dir = self.output_base_path / folder_name
                    output_dir.mkdir(parents=True, exist_ok=True)
                    nemotecnia_base = self.extract_base_nemotecnia(full_nemotecnia, channel_suffix)
                    sql_date = self.format_date_for_sql(exec_date_raw)
                    filename = f"{nemotecnia_base}{exec_date_raw}{channel_suffix}.txt"
                    file_path = output_dir / filename
                    logger.info(f"Procesando: {full_nemotecnia} para fecha {sql_date} en carpeta {folder_name}")
                    query_template = self.get_query_template(channel_name)
                    query = query_template.format(execution_date=sql_date, full_nemotecnia=full_nemotecnia)
                    description, rows = db.execute_query(query)
                    headers = [desc[0] for desc in description] if description else []
                    count = len(rows)
                    logger.info(f"Registros encontrados: {count}")
                    if count < self.min_records:
                        logger.warning(
                            f"Registros insuficientes ({count}) para {full_nemotecnia}. "
                            f"Mínimo requerido: {self.min_records}. Archivo NO generado."
                        )
                        continue
                    if not self._confirm_overwrite(file_path, count):
                        logger.info(f"Archivo omitido por el usuario: {filename}")
                        continue
                    with open(file_path, 'w', encoding='utf-8') as f:
                        if headers:
                            f.write("|".join(headers) + "\n")
                        for row_data in rows:
                            line = "|".join([str(item) if item is not None else '' for item in row_data])
                            f.write(line + "\n")
                    logger.info(f"Archivo generado: {filename} ({count} registros)")
                except Exception as e:
                    logger.error(f"Error procesando fila {index}: {e}")
                    continue
        except Exception as e:
            logger.critical(f"Error crítico en el proceso: {e}")
            raise
        finally:
            db.disconnect()
            logger.info("Proceso finalizado.")
