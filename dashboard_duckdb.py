#!/usr/bin/env python3
"""
Dashboard simple con Dash y DuckDB usando Polars
Uso: python dashboard_duckdb.py
"""

import duckdb
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, dash_table
import dash_bootstrap_components as dbc
from datetime import datetime

DB_FILE = 'air_quality.duckdb'

def load_data():
    """Carga datos de DuckDB usando Polars"""
    try:
        # Conexion a DuckDB
        conn = duckdb.connect(DB_FILE, read_only=True)
        
        # Verificar que las tablas existen
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        if 'measurements' not in table_names:
            print(f"Error: La tabla 'measurements' no existe en la base de datos.")
            print(f"   Tablas disponibles: {table_names}")
            print("Ejecuta primero: python load_to_duckdb.py")
            conn.close()
            exit(1)
        
        # Query principal con joins
        query = '''
            SELECT 
                m.timestamp,
                m.value,
                m.station_id,
                s.station_name,
                m.monitor_code,
                mon.monitor_name,
                mon.unit
            FROM measurements m
            LEFT JOIN stations s ON m.station_id = s.station_id
            LEFT JOIN monitors mon ON m.monitor_code = mon.monitor_code
            WHERE m.value IS NOT NULL
            ORDER BY m.timestamp
        '''
        
        # Obtener DataFrame de Polars directamente desde DuckDB
        df = conn.execute(query).pl()
        
        print(f"Query ejecutado: {len(df)} filas")
        
        # Verificar que timestamp es datetime (deberia serlo ya)
        if df['timestamp'].dtype != pl.Datetime:
            df = df.with_columns(pl.col('timestamp').cast(pl.Datetime))
        
        # Stats Query
        stats_query = '''
            SELECT 
                COUNT(*) as total_measurements,
                COUNT(DISTINCT station_id) as total_stations,
                COUNT(DISTINCT monitor_code) as total_monitors,
                MIN(timestamp) as first_date,
                MAX(timestamp) as last_date
            FROM measurements
            WHERE value IS NOT NULL
        '''
        stats = conn.execute(stats_query).pl()
        
        print(f"Stats calculados: {stats['total_measurements'].item():,} mediciones")
        
        conn.close()
        return df, stats
        
    except duckdb.CatalogException as e:
        print(f"Error: La base de datos DuckDB '{DB_FILE}' no existe o faltan tablas.")
        print(f"   Detalle: {e}")
        print("Ejecuta primero: python load_to_duckdb.py")
        exit(1)
    except Exception as e:
        print(f"Error cargando datos desde DuckDB: {e}")
        import traceback
        traceback.print_exc()
        return pl.DataFrame(), pl.DataFrame()

# Cargar datos
print("Cargando datos desde DuckDB...")
df, stats = load_data()

if df.is_empty():
    print("No hay datos en la base de datos")
    print("Ejecuta primero: python load_to_duckdb.py")
    exit(1)

print(f"Cargados {len(df):,} registros")

# Crear app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Calidad del Aire - Bogota (DuckDB + Polars)"

# Opciones para dropdowns - convertir a listas de Python
stations = sorted([s for s in df['station_name'].unique().to_list() if s is not None])
monitors = sorted([m for m in df['monitor_name'].unique().to_list() if m is not None])

print(f"Estaciones disponibles: {len(stations)}")
print(f"Monitores disponibles: {len(monitors)}")

# Layout
app.layout = dbc.Container([
    # Header
    dbc.Row([
        dbc.Col([
            html.H1("Calidad del Aire - Bogota", className='text-center text-primary mb-4'),
            html.Hr()
        ])
    ]),
    
    # Stats cards
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(f"{stats['total_measurements'].item():,}", className='text-info'),
                    html.P("Mediciones totales", className='text-muted')
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(f"{stats['total_stations'].item()}", className='text-success'),
                    html.P("Estaciones", className='text-muted')
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(f"{stats['total_monitors'].item()}", className='text-warning'),
                    html.P("Monitores", className='text-muted')
                ])
            ])
        ], width=3),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4(
                        f"{(stats['last_date'].item() - stats['first_date'].item()).days}", 
                        className='text-danger'
                    ),
                    html.P("Dias de datos", className='text-muted')
                ])
            ])
        ], width=3)
    ], className='mb-4'),
    
    # Filtros
    dbc.Row([
        dbc.Col([
            html.Label("Estacion:", className='fw-bold'),
            dcc.Dropdown(
                id='station-dropdown',
                options=[{'label': s, 'value': s} for s in stations],
                value=stations[0] if stations else None,
                clearable=False
            )
        ], width=6),
        dbc.Col([
            html.Label("Monitor:", className='fw-bold'),
            dcc.Dropdown(
                id='monitor-dropdown',
                options=[{'label': m, 'value': m} for m in monitors],
                value=monitors[0] if monitors else None,
                clearable=False
            )
        ], width=6)
    ], className='mb-4'),
    
    # Grafico principal
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='time-series', config={'displayModeBar': True})
        ])
    ], className='mb-4'),
    
    # Graficos secundarios
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='histogram')
        ], width=6),
        dbc.Col([
            dcc.Graph(id='boxplot')
        ], width=6)
    ], className='mb-4'),
    
    # Tabla de estadisticas
    dbc.Row([
        dbc.Col([
            html.H5("Estadisticas Descriptivas", className='text-center mb-3'),
            html.Div(id='stats-table')
        ])
    ])
    
], fluid=True, className='p-4')

# Callbacks - ahora usando Polars
@app.callback(
    [Output('time-series', 'figure'),
     Output('histogram', 'figure'),
     Output('boxplot', 'figure'),
     Output('stats-table', 'children')],
    [Input('station-dropdown', 'value'),
     Input('monitor-dropdown', 'value')]
)
def update_graphs(station, monitor):
    # Filtrar datos usando Polars
    filtered = df.filter(
        (pl.col('station_name') == station) & 
        (pl.col('monitor_name') == monitor)
    )
    
    if filtered.is_empty():
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="No hay datos para esta combinacion", 
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False
        )
        return empty_fig, empty_fig, empty_fig, html.P("Sin datos")
    
    # Ordenar por fecha
    filtered = filtered.sort('timestamp')
    
    # Obtener unidad
    unit = filtered['unit'][0] if len(filtered) > 0 and filtered['unit'][0] is not None else ""
    
    # Intentar con Polars directo primero, fallback a graph_objects si falla
    try:
        # Serie temporal
        fig_line = px.line(
            filtered,
            x='timestamp',
            y='value',
            title=f'Serie Temporal - {station} - {monitor}',
            labels={'timestamp': 'Fecha', 'value': f'Valor ({unit})'}
        )
        fig_line.update_layout(
            hovermode='x unified',
            xaxis_title='Fecha',
            yaxis_title=f'Valor ({unit})'
        )
        fig_line.update_traces(line_color='#1f77b4')
        
        # Histograma
        fig_hist = px.histogram(
            filtered,
            x='value',
            nbins=30,
            title='Distribucion de Valores',
            labels={'value': f'Valor ({unit})'}
        )
        fig_hist.update_traces(marker_color='#2ca02c')
        fig_hist.update_layout(xaxis_title=f'Valor ({unit})', yaxis_title='Frecuencia')
        
        # Boxplot por dia de la semana
        day_map = {
            0: 'Lunes', 1: 'Martes', 2: 'Miercoles', 
            3: 'Jueves', 4: 'Viernes', 5: 'Sabado', 6: 'Domingo'
        }
        
        filtered_box = filtered.with_columns([
            pl.col('timestamp').dt.weekday().alias('weekday_num'),
        ])
        
        filtered_box = filtered_box.with_columns([
            pl.col('weekday_num').map_elements(lambda x: day_map.get(x, ''), return_dtype=pl.Utf8).alias('day_of_week_es')
        ])
        
        day_order_es = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
        
        fig_box = px.box(
            filtered_box,
            x='day_of_week_es',
            y='value',
            title='Distribucion por Dia de la Semana',
            labels={'day_of_week_es': 'Dia', 'value': f'Valor ({unit})'},
            category_orders={'day_of_week_es': day_order_es}
        )
        fig_box.update_traces(marker_color='#ff7f0e')
        fig_box.update_layout(xaxis_title='Dia de la Semana', yaxis_title=f'Valor ({unit})')
        
    except (NotImplementedError, TypeError, ModuleNotFoundError):
        # Fallback a plotly.graph_objects
        print("Usando fallback a graph_objects ")
        
        # Serie temporal
        fig_line = go.Figure()
        fig_line.add_trace(go.Scatter(
            x=filtered['timestamp'].to_list(),
            y=filtered['value'].to_list(),
            mode='lines',
            line=dict(color='#1f77b4')
        ))
        fig_line.update_layout(
            title=f'Serie Temporal - {station} - {monitor}',
            xaxis_title='Fecha',
            yaxis_title=f'Valor ({unit})',
            hovermode='x unified'
        )
        
        # Histograma
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=filtered['value'].to_list(),
            nbinsx=30,
            marker_color='#2ca02c'
        ))
        fig_hist.update_layout(
            title='Distribucion de Valores',
            xaxis_title=f'Valor ({unit})',
            yaxis_title='Frecuencia'
        )
        
        # Boxplot
        day_map = {
            0: 'Lunes', 1: 'Martes', 2: 'Miercoles', 
            3: 'Jueves', 4: 'Viernes', 5: 'Sabado', 6: 'Domingo'
        }
        
        filtered_box = filtered.with_columns([
            pl.col('timestamp').dt.weekday().alias('weekday_num'),
        ])
        
        filtered_box = filtered_box.with_columns([
            pl.col('weekday_num').map_elements(lambda x: day_map.get(x, ''), return_dtype=pl.Utf8).alias('day_of_week_es')
        ])
        
        day_order_es = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
        
        fig_box = go.Figure()
        for day in day_order_es:
            day_data = filtered_box.filter(pl.col('day_of_week_es') == day)
            if not day_data.is_empty():
                fig_box.add_trace(go.Box(
                    y=day_data['value'].to_list(),
                    name=day,
                    marker_color='#ff7f0e'
                ))
        fig_box.update_layout(
            title='Distribucion por Dia de la Semana',
            xaxis_title='Dia de la Semana',
            yaxis_title=f'Valor ({unit})'
        )
    
    # Estadisticas usando Polars
    stats_pl = filtered.select([
        pl.col('value').count().alias('count'),
        pl.col('value').mean().alias('mean'),
        pl.col('value').std().alias('std'),
        pl.col('value').min().alias('min'),
        pl.col('value').quantile(0.25).alias('q25'),
        pl.col('value').median().alias('median'),
        pl.col('value').quantile(0.75).alias('q75'),
        pl.col('value').max().alias('max')
    ])
    
    stats_dict = {
        'Estadistica': ['Conteo', 'Promedio', 'Desv. Estandar', 'Minimo', '25%', 'Mediana', '75%', 'Maximo'],
        'Valor': [
            f"{stats_pl['count'][0]:.0f}",
            f"{stats_pl['mean'][0]:.2f}",
            f"{stats_pl['std'][0]:.2f}",
            f"{stats_pl['min'][0]:.2f}",
            f"{stats_pl['q25'][0]:.2f}",
            f"{stats_pl['median'][0]:.2f}",
            f"{stats_pl['q75'][0]:.2f}",
            f"{stats_pl['max'][0]:.2f}"
        ]
    }
    
    table = dash_table.DataTable(
        data=[{'Estadistica': k, 'Valor': v} for k, v in zip(stats_dict['Estadistica'], stats_dict['Valor'])],
        columns=[{'name': 'Estadistica', 'id': 'Estadistica'}, {'name': 'Valor', 'id': 'Valor'}],
        style_cell={'textAlign': 'left', 'padding': '10px'},
        style_header={'backgroundColor': '#007bff', 'color': 'white', 'fontWeight': 'bold'},
        style_data_conditional=[
            {'if': {'row_index': 'odd'}, 'backgroundColor': '#f8f9fa'}
        ]
    )
    
    return fig_line, fig_hist, fig_box, table

if __name__ == '__main__':
    print("\n" + "="*60)
    print("Dashboard iniciado correctamente")
    print("="*60)
    print(f"Datos cargados: {len(df):,} mediciones")
    print(f"Estaciones: {len(stations)}")
    print(f"Monitores: {len(monitors)}")
    print(f"Rango: {df['timestamp'].min()} -> {df['timestamp'].max()}")
    print("\nAbre tu navegador en: http://localhost:8050")
    print("="*60 + "\n")
    
    app.run(debug=True, port=8050)