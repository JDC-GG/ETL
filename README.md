
##  Requisitos

- Python 3.10+
- pip
- Visual Studio Code
- Robot Framework
- Polars
- Duck DB 
- Librerías adicionales:

```bash
pip install -r requirements.txt
```

---

##  Estructura

- `download_report.robot`: Ejecuta la descarga desde la API de RMCAB.
- `resources/utils.py`: Funciones auxiliares para conversión de fechas y escritura de archivos.
- `appsettings.json`: Configuración de parámetros de consulta.

---

##  Ejecución rápida

1. Clona el proyecto y entra al directorio:

```bash
cd ETL
```

2. Crea y activa un entorno virtual:

```bash
python -m venv .venv
source .venv/Scripts/activate     # Linux
```

3. Instala dependencias:

```bash
pip install -r requirements.txt
```

4. Ejecuta todo el flujo (descarga):

```bash
robot download_report.robot
python load_to_duckdb.py
python dashboard_duckdb.py
```

## 🛠 Configuración

Edita `appsettings.json` para personalizar parámetros de estación, fechas y salida. 

Estaciones : 4, 13, 27, 3, 5, 37, 38, 30, 8, 34, 9, 6, 17, 26, 39, 24, 11, 1, 32

