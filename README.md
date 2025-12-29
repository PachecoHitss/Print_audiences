# Sistema de Generación de Audiencias (Print Audiences)

Este proyecto es una herramienta de automatización desarrollada en Python para generar archivos de audiencias de marketing mediante consultas a una base de datos Teradata. El sistema lee un archivo de entrada con definiciones de campañas, ejecuta consultas SQL específicas según el canal (Email, SMS, Push, etc.) y exporta los resultados a archivos planos.

## 📋 Requisitos Previos

*   **Python 3.x** instalado.
*   Acceso a la base de datos **Teradata**.
*   Archivo de configuración `Config.json` con las credenciales (ver sección [Configuración](#configuración)).
*   Archivo de entrada CSV en la ruta esperada.

## 🚀 Instalación

1.  Clona este repositorio o descarga el código fuente.
2.  Crea un entorno virtual (recomendado):
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # En Windows
    ```
3.  Instala las dependencias necesarias:
    ```bash
    pip install -r requirements.txt
    ```

## ⚙️ Configuración

### 1. Credenciales de Base de Datos (`Config.json`)
El sistema busca un archivo llamado `Config.json` **dos niveles arriba** de la carpeta `src` (es decir, en la carpeta padre de donde se encuentra este proyecto, por ejemplo `D:\LABORAL\Projects\Config.json`).

Este archivo debe tener la siguiente estructura:

```json
{
    "teradata": {
        "TD_HOST": "tu_host_teradata",
        "TD_USER": "tu_usuario",
        "TD_PASSWORD": "tu_contraseña"
    }
}
```

### 2. Rutas de Archivos (Hardcoded)
Actualmente, las rutas de entrada y salida están definidas directamente en el código (`src/processor.py`). Asegúrate de que estas rutas existan o modifícalas en el código si es necesario:

*   **Entrada (CSV):** `D:\LABORAL\Projects\Project_A\data\printAudiencesData\PrintAudiencesData.csv`
*   **Salida (Resultados):** `D:\OneDrive - Comunicacion Celular S.A.- Comcel S.A\Archivos de GCCO CG - Bases Certificadas\especificas`

## ▶️ Ejecución

Para iniciar el proceso de generación de audiencias, ejecuta el archivo principal desde la terminal:

```bash
python main.py
```

### Flujo de Ejecución:
1.  El sistema lee el archivo de entrada CSV.
2.  Por cada registro, determina el tipo de canal (Email, Push, SMS, etc.).
3.  Construye y ejecuta la consulta SQL correspondiente en Teradata.
4.  Guarda los resultados en la carpeta de salida.
    *   ⚠️ **Nota:** Si el archivo de salida ya existe, el sistema pedirá confirmación en la consola para sobrescribirlo (`s/n`).

## 📂 Estructura del Proyecto

```
Print_audiences/
├── logs/               # Archivos de log de ejecución
├── src/
│   ├── config.py       # Carga de configuración (Config.json)
│   ├── database.py     # Gestión de conexión a Teradata
│   ├── processor.py    # Lógica principal de procesamiento de audiencias
│   └── test_connection.py
├── main.py             # Punto de entrada del script
├── requirements.txt    # Dependencias del proyecto
└── README.md           # Documentación
```

## 📝 Logs
Los detalles de la ejecución y posibles errores se registran en:
*   Consola (salida estándar)
*   Archivo: `logs/execution.log`
