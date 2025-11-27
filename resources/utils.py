import json
from datetime import datetime, timedelta

# Nota importante sobre ticks .NET:
# El endpoint usa ticks de .NET (100 ns desde 0001-01-01).
# Aquí calculamos ticks a partir de FECHAS LOCALES "naive" (sin convertir a UTC),
# Si ves un desfase horario, cambia a la versión UTC comentada más abajo.

_DOTNET_EPOCH = datetime(1, 1, 1)

def to_dotnet_ticks(dt_str: str, tz: str = "America/Bogota") -> int:
    """
    dt_str: 'YYYY-MM-DD HH:MM' (hora local). 
    Retorna ticks .NET como entero.
    """
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")  # naive local
    delta = dt - _DOTNET_EPOCH
    ticks = int(delta.total_seconds() * 10_000_000)
    return ticks

def ticks_to_iso(ticks: int, tz: str = "America/Bogota") -> str:
    """
    Convierte ticks .NET a 'YYYY-MM-DD HH:MM'.
    (Solo para armar nombres de archivo legibles)
    """
    dt = _DOTNET_EPOCH + timedelta(microseconds=ticks / 10)
    return dt.strftime("%Y-%m-%d %H:%M")

def dumps_list_as_string(py_list) -> str:
    """
    Devuelve un string JSON del tipo ["A","B","C"].
    Requests/urllib lo url-encodea correctamente en la query.
    """
    return json.dumps(py_list, ensure_ascii=False)
