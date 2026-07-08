import json
from pathlib import Path


class ConfigLoader:
    def __init__(self):
        self.base_path = Path(__file__).resolve().parent.parent
        self.config_path = self.base_path.parent / 'Config.json'
        self._config = self._load_config()

    def _load_config(self):
        if not self.config_path.exists():
            raise FileNotFoundError(f"No se encontró el archivo de configuración en: {self.config_path}")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error al decodificar Config.json: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado leyendo configuración: {e}")

    def get_teradata_creds(self):
        td_config = self._config.get('teradata', {})
        required_keys = ['TD_HOST', 'TD_USER', 'TD_PASSWORD']
        missing = [key for key in required_keys if key not in td_config]
        if missing:
            raise ValueError(f"Faltan credenciales de Teradata en Config.json: {missing}")
        return td_config

    def get_paths(self):
        paths = self._config.get('paths', {})
        required_keys = ['input_file', 'output_base', 'servicio_base']
        missing = [key for key in required_keys if key not in paths]
        if missing:
            raise ValueError(f"Faltan rutas en Config.json ['paths']: {missing}")
        return paths

    def get_settings(self):
        return self._config.get('settings', {})


_config_loader = None


def get_config() -> ConfigLoader:
    """Devuelve la instancia única de ConfigLoader (lazy initialization)."""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader
