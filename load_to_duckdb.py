
"""
Carga datos JSON a DuckDB usando Polars (sin SQLite ni Pandas)
Uso: python load_to_duckdb.py
"""

import json
import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timedelta
import sys
import re

DB_FILE = 'air_quality.duckdb'

def create_database():
    """Crea base de datos DuckDB con esquema"""
    conn = duckdb.connect(DB_FILE)
    
    # DuckDB es más flexible con tipos de datos
    conn.execute('''
        CREATE TABLE IF NOT EXISTS stations (
            station_id INTEGER PRIMARY KEY,
            station_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS monitors (
            monitor_id INTEGER PRIMARY KEY,
            monitor_code VARCHAR UNIQUE,
            monitor_name VARCHAR,
            unit VARCHAR
        )
    ''')
    
    conn.execute('''
        CREATE SEQUENCE IF NOT EXISTS monitor_seq START 1
    ''')
    
    conn.execute('''
        CREATE SEQUENCE IF NOT EXISTS measurement_seq START 1
    ''')
    
    conn.execute('''
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY,
            station_id INTEGER,
            monitor_code VARCHAR,
            timestamp TIMESTAMP NOT NULL,
            value DOUBLE,
            report_type VARCHAR,
            granularity_minutes INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Crear índice único compuesto para evitar duplicados
    conn.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_unique 
        ON measurements(station_id, monitor_code, timestamp, report_type)
    ''')
    
    # Índices para performance
    conn.execute('CREATE INDEX IF NOT EXISTS idx_measurements_station ON measurements(station_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_measurements_timestamp ON measurements(timestamp)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_measurements_monitor ON measurements(monitor_code)')
    
    print(" Base de datos DuckDB creada")
    return conn

def load_code_map():
    """Carga mapeo de códigos a nombres legibles"""
    code_map_path = Path('config/code_title_map.json')
    if code_map_path.exists():
        with open(code_map_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            flat_map = {}
            for station_id, station_info in data.get('stations', {}).items():
                for code, info in station_info.get('codes', {}).items():
                    flat_map[code] = info
            return flat_map
    return {}

def parse_filename(filename):
    """Extrae station_id del nombre de archivo"""
    match = re.match(r'^(\d+)_', filename)
    if match:
        return int(match.group(1))
    return None

def normalize_datetime(dt_str):
    """Normaliza formato DD-MM-YYYY HH:MM a YYYY-MM-DD HH:MM"""
    try:
        match = re.match(r'^(\d{2})-(\d{2})-(\d{4}) (\d{2}):(\d{2})$', dt_str)
        if not match:
            return None
        
        day, month, year, hour, minute = map(int, match.groups())
        
        # Manejar hora 24:00
        if hour == 24:
            dt = datetime(year, month, day, 0, minute) + timedelta(days=1)
        else:
            dt = datetime(year, month, day, hour, minute)
        
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        print(f" Error parseando fecha '{dt_str}': {e}")
        return None

def is_valid_measurement_row(row):
    """Verifica si la fila contiene datos reales (no es summary)"""
    datetime_val = row.get('datetime', '')
    
    summary_keywords = ['Summary', 'Minimum', 'MinDate', 'MinTime', 
                       'Maximum', 'MaxDate', 'MaxTime', 'Avg', 'Num', 
                       'DataPrecent', 'STD', 'Count']
    
    if any(keyword in str(datetime_val) for keyword in summary_keywords):
        return False
    
    if not re.match(r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$', str(datetime_val)):
        return False
    
    return True

def parse_json_to_polars(json_data, code_map, station_id):
    """
    Parsea JSON y retorna DataFrames de Polars
    Retorna: (measurements_df, monitors_dict)
    """
    
    if not isinstance(json_data, list):
        print(f"  JSON no es un array. Tipo: {type(json_data)}")
        if isinstance(json_data, dict):
            for key in ['data', 'Data', 'records', 'Records']:
                if key in json_data and isinstance(json_data[key], list):
                    json_data = json_data[key]
                    print(f" Encontrada sección de datos en clave '{key}'")
                    break
        
        if not isinstance(json_data, list):
            return pl.DataFrame(), {}
    
    print(f" Encontradas {len(json_data)} filas totales")
    
    # Filtrar solo filas válidas
    valid_rows = [row for row in json_data if is_valid_measurement_row(row)]
    print(f" Filas válidas (sin sumarios): {len(valid_rows)}")
    
    # Preparar datos para Polars
    measurements_list = []
    monitors_dict = {}
    
    for row in valid_rows:
        datetime_str = row.get('datetime', '')
        timestamp = normalize_datetime(datetime_str)
        
        if not timestamp:
            continue
        
        for key, value in row.items():
            if key.startswith('S_') and key != 'datetime':
                monitor_code = key
                
                # Registrar monitor
                if monitor_code not in monitors_dict:
                    monitor_info = code_map.get(monitor_code, {})
                    monitors_dict[monitor_code] = {
                        'code': monitor_code,
                        'name': monitor_info.get('label', monitor_code),
                        'unit': monitor_info.get('unit', '')
                    }
                
                # Convertir valor
                if value in [None, '', '-', '----', 'N/A', 'NaN']:
                    value = None
                else:
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        value = None
                
                measurements_list.append({
                    'station_id': station_id,
                    'monitor_code': monitor_code,
                    'timestamp': timestamp,
                    'value': value
                })
    
    # Crear DataFrame de Polars
    if measurements_list:
        df = pl.DataFrame(measurements_list)
        # Convertir timestamp a tipo datetime
        df = df.with_columns(pl.col('timestamp').str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"))
    else:
        df = pl.DataFrame()
    
    print(f" Procesadas {len(measurements_list)} mediciones")
    print(f" Encontrados {len(monitors_dict)} monitores únicos")
    
    return df, monitors_dict

def insert_data_duckdb(conn, measurements_df, station_id, station_name, monitors_dict, report_type='Average', granularity=60):
    """Inserta datos en DuckDB usando Polars"""
    
    # Insertar estación (ON CONFLICT con PRIMARY KEY)
    conn.execute('''
        INSERT INTO stations (station_id, station_name) 
        VALUES (?, ?)
        ON CONFLICT (station_id) DO UPDATE SET station_name = EXCLUDED.station_name
    ''', [station_id, station_name])
    print(f" Insertada estación: {station_name} (ID: {station_id})")
    
    # Insertar monitores (ON CONFLICT con UNIQUE constraint)
    for mcode, minfo in monitors_dict.items():
        conn.execute('''
            INSERT INTO monitors (monitor_id, monitor_code, monitor_name, unit) 
            VALUES (nextval('monitor_seq'), ?, ?, ?)
            ON CONFLICT (monitor_code) DO UPDATE SET 
                monitor_name = EXCLUDED.monitor_name,
                unit = EXCLUDED.unit
        ''', [minfo['code'], minfo['name'], minfo['unit']])
    print(f" Insertados {len(monitors_dict)} monitores")
    
    # Preparar DataFrame con columnas adicionales
    if not measurements_df.is_empty():
        measurements_df = measurements_df.with_columns([
            pl.lit(report_type).alias('report_type'),
            pl.lit(granularity).alias('granularity_minutes'),
            pl.lit(datetime.now()).alias('created_at')
        ])
        
        # SOLUCIÓN: Usar INSERT con ON CONFLICT especificando el índice único
        try:
            # Registrar el DataFrame de Polars en DuckDB
            conn.register('temp_measurements', measurements_df)
            
            # Insertar con manejo de conflictos usando el índice único compuesto
            conn.execute('''
                INSERT INTO measurements 
                (id, station_id, monitor_code, timestamp, value, report_type, granularity_minutes, created_at)
                SELECT 
                    nextval('measurement_seq'),
                    station_id, 
                    monitor_code, 
                    timestamp, 
                    value, 
                    report_type, 
                    granularity_minutes,
                    created_at
                FROM temp_measurements
                ON CONFLICT (station_id, monitor_code, timestamp, report_type) 
                DO UPDATE SET 
                    value = EXCLUDED.value,
                    granularity_minutes = EXCLUDED.granularity_minutes,
                    created_at = EXCLUDED.created_at
            ''')
            
            # Limpiar tabla temporal
            conn.unregister('temp_measurements')
            
            print(f" Insertadas {len(measurements_df)} mediciones")
        except Exception as e:
            print(f"  Error insertando mediciones: {e}")
            import traceback
            traceback.print_exc()

def process_json_file(filepath, conn, code_map, config):
    """Procesa un archivo JSON"""
    print(f"\n{'='*60}")
    print(f" Procesando: {filepath.name}")
    print(f"{'='*60}")
    
    station_id = parse_filename(filepath.name)
    if station_id is None:
        print(f" No se pudo extraer station_id del nombre: {filepath.name}")
        return
    
    parts = filepath.stem.split('_')
    station_name = parts[1] if len(parts) > 1 else f"Estación {station_id}"
    
    print(f" Estación: {station_name} (ID: {station_id})")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f" Error parseando JSON: {e}")
        return
    
    measurements_df, monitors_dict = parse_json_to_polars(data, code_map, station_id)
    
    if measurements_df.is_empty():
        print("  No se encontraron mediciones válidas")
        return
    
    report_type = config.get('report', {}).get('type', 'Average')
    granularity = config.get('time', {}).get('granularity_minutes', 60)
    
    insert_data_duckdb(conn, measurements_df, station_id, station_name, monitors_dict, report_type, granularity)

def main():
    print("\n" + "="*60)
    print(" Iniciando carga a DuckDB con Polars...")
    print("="*60 + "\n")
    
    # Crear/conectar a base de datos
    conn = create_database()
    
    # Cargar configuración
    config = {}
    config_path = Path('config/appsettings.json')
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    
    # Cargar mapeo de códigos
    code_map = load_code_map()
    print(f" Cargados {len(code_map)} códigos de monitores\n")
    
    # Procesar archivos JSON
    download_dir = Path('downloads')
    if not download_dir.exists():
        print(f" Directorio no existe: {download_dir}")
        print(" Ejecuta primero: robot download_report.robot")
        sys.exit(1)
    
    json_files = list(download_dir.glob('*.json'))
    if not json_files:
        print(f"  No hay archivos JSON en {download_dir}")
        print(" Ejecuta primero: robot download_report.robot")
        sys.exit(0)
    
    print(f" Encontrados {len(json_files)} archivos JSON\n")
    
    success_count = 0
    error_count = 0
    
    for json_file in sorted(json_files):
        try:
            process_json_file(json_file, conn, code_map, config)
            success_count += 1
        except Exception as e:
            error_count += 1
            print(f" Error procesando {json_file.name}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Resumen final usando DuckDB
    total_measurements = conn.execute('SELECT COUNT(*) FROM measurements').fetchone()[0]
    total_stations = conn.execute('SELECT COUNT(*) FROM stations').fetchone()[0]
    total_monitors = conn.execute('SELECT COUNT(*) FROM monitors').fetchone()[0]
    
    date_range = conn.execute(
        'SELECT MIN(timestamp), MAX(timestamp) FROM measurements WHERE timestamp IS NOT NULL'
    ).fetchone()
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(" CARGA COMPLETADA")
    print(f"{'='*60}")
    print(f" Total mediciones: {total_measurements:,}")
    print(f" Total estaciones: {total_stations}")
    print(f" Total monitores: {total_monitors}")
    if date_range[0]:
        print(f" Rango de fechas: {date_range[0]} → {date_range[1]}")
    print(f" Archivos procesados exitosamente: {success_count}")
    if error_count > 0:
        print(f"  Archivos con errores: {error_count}")
    print(f" Base de datos: {DB_FILE}")
    print(f"\n Siguiente paso: python dashboard_duckdb.py\n")

if __name__ == '__main__':
    main()