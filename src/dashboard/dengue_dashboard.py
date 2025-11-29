import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import duckdb
from pathlib import Path
import joblib

# Initialize app
app = dash.Dash(__name__)

# Load data and model
base_dir = Path(r"D:\_data-science\GitHub\eco-sentinel")
model_path = base_dir / "models/dengue_predictor_rf.pkl"
predictions_path = base_dir / "models/model_predictions.parquet"
gold_path = base_dir / "data/gold/dengue_clima.parquet"

# Load model
model = joblib.load(model_path)

# Load predictions
df_predictions = pd.read_parquet(predictions_path)

# Load recent data for monitoring
con = duckdb.connect(database=':memory:')
df_recent = con.execute(f"""
    SELECT geocode, nome_municipio, uf, semana_epidemiologica, 
           casos_notificados, inmet_temp_media, inmet_precip_tot,
           inmet_temp_media_lag1, inmet_temp_media_lag2, 
           inmet_precip_tot_lag1, inmet_precip_tot_lag2
    FROM read_parquet('{gold_path}')
    WHERE CAST(semana_epidemiologica AS VARCHAR) LIKE '2024%'
    ORDER BY semana_epidemiologica DESC
    LIMIT 1000
""").df()

con.close()

# App layout
app.layout = html.Div([
    html.H1("🦟 Dashboard de Monitoramento - Dengue & Clima", className="header"),
    
    html.Div([
        html.Div([
            html.H3("Alertas de Risco"),
            html.Div(id="alert-container")
        ], className="alert-section"),
        
        html.Div([
            html.H3("Previsões vs Realidade"),
            dcc.Graph(id="prediction-chart")
        ], className="prediction-section"),
        
        html.Div([
            html.H3("Análise Temporal"),
            dcc.Graph(id="temporal-chart")
        ], className="temporal-section"),
        
        html.Div([
            html.H3("Mapa de Calor - Correlações"),
            dcc.Graph(id="correlation-heatmap")
        ], className="correlation-section"),
        
        html.Div([
            html.H3("Filtros"),
            html.Label("Estado:"),
            dcc.Dropdown(
                id="uf-filter",
                options=[{"label": uf, "value": uf} for uf in sorted(df_recent['uf'].unique())],
                value="SP",
                multi=False
            ),
            html.Br(),
            html.Label("Município:"),
            dcc.Dropdown(
                id="municipio-filter",
                multi=False
            )
        ], className="filter-section")
    ], className="dashboard-container")
], className="main-container")

# Callbacks
@app.callback(
    Output("municipio-filter", "options"),
    Input("uf-filter", "value")
)
def update_municipio_options(selected_uf):
    municipios = df_recent[df_recent['uf'] == selected_uf]['nome_municipio'].unique()
    return [{"label": mun, "value": mun} for mun in sorted(municipios)]

@app.callback(
    Output("alert-container", "children"),
    Input("uf-filter", "value"),
    Input("municipio-filter", "value")
)
def update_alerts(selected_uf, selected_municipio):
    # Filter data
    filtered_df = df_recent.copy()
    if selected_uf:
        filtered_df = filtered_df[filtered_df['uf'] == selected_uf]
    if selected_municipio:
        filtered_df = filtered_df[filtered_df['nome_municipio'] == selected_municipio]
    
    # Calculate risk indicators
    recent_cases = filtered_df.groupby('nome_municipio')['casos_notificados'].sum().reset_index()
    recent_cases = recent_cases.sort_values('casos_notificados', ascending=False).head(5)
    
    # Create alerts
    alerts = []
    for _, row in recent_cases.iterrows():
        if row['casos_notificados'] > 500:
            risk_level = "🔴 ALTO"
            color = "red"
        elif row['casos_notificados'] > 100:
            risk_level = "🟡 MÉDIO"
            color = "orange"
        else:
            risk_level = "🟢 BAIXO"
            color = "green"
            
        alerts.append(html.Div([
            html.Strong(f"{row['nome_municipio']}: "),
            html.Span(risk_level, style={"color": color}),
            html.Span(f" ({row['casos_notificados']} casos)")
        ], className="alert-item"))
    
    return alerts if alerts else html.P("Nenhum alerta de risco identificado.")

@app.callback(
    Output("prediction-chart", "figure"),
    Input("uf-filter", "value")
)
def update_prediction_chart(selected_uf):
    # Filter predictions by state
    filtered_df = df_predictions.copy()
    if selected_uf:
        filtered_df = filtered_df[filtered_df['uf'] == selected_uf]
    
    # Sample data for visualization
    sample_df = filtered_df.sample(min(1000, len(filtered_df)))
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("Previsões vs Casos Reais", "Distribuição de Erros"),
        specs=[[{"secondary_y": False}, {"type": "histogram"}]]
    )
    
    # Scatter plot
    fig.add_trace(
        go.Scatter(
            x=sample_df['actual_cases'],
            y=sample_df['predicted_cases'],
            mode='markers',
            name='Previsões',
            marker=dict(color='blue', opacity=0.6)
        ),
        row=1, col=1
    )
    
    # Perfect prediction line
    fig.add_trace(
        go.Scatter(
            x=[0, sample_df['actual_cases'].max()],
            y=[0, sample_df['actual_cases'].max()],
            mode='lines',
            name='Previsão Perfeita',
            line=dict(color='red', dash='dash')
        ),
        row=1, col=1
    )
    
    # Error histogram
    fig.add_trace(
        go.Histogram(
            x=sample_df['error_pct'],
            nbinsx=30,
            name='Erro (%)',
            marker_color='lightblue'
        ),
        row=1, col=2
    )
    
    fig.update_xaxes(title_text="Casos Reais", row=1, col=1)
    fig.update_yaxes(title_text="Casos Previstos", row=1, col=1)
    fig.update_xaxes(title_text="Erro Percentual", row=1, col=2)
    fig.update_yaxes(title_text="Frequência", row=1, col=2)
    
    fig.update_layout(height=400, showlegend=True)
    
    return fig

@app.callback(
    Output("temporal-chart", "figure"),
    Input("uf-filter", "value"),
    Input("municipio-filter", "value")
)
def update_temporal_chart(selected_uf, selected_municipio):
    # Filter data
    filtered_df = df_recent.copy()
    if selected_uf:
        filtered_df = filtered_df[filtered_df['uf'] == selected_uf]
    if selected_municipio:
        filtered_df = filtered_df[filtered_df['nome_municipio'] == selected_municipio]
    
    # Aggregate by week
    temporal_df = filtered_df.groupby('semana_epidemiologica').agg({
        'casos_notificados': 'sum',
        'inmet_temp_media': 'mean',
        'inmet_precip_tot': 'mean'
    }).reset_index()
    
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Casos de Dengue por Semana", "Condições Climáticas"),
        specs=[[{"secondary_y": False}], [{"secondary_y": True}]]
    )
    
    # Cases plot
    fig.add_trace(
        go.Bar(
            x=temporal_df['semana_epidemiologica'],
            y=temporal_df['casos_notificados'],
            name="Casos",
            marker_color='red'
        ),
        row=1, col=1
    )
    
    # Climate plot
    fig.add_trace(
        go.Scatter(
            x=temporal_df['semana_epidemiologica'],
            y=temporal_df['inmet_temp_media'],
            name="Temp. Média (°C)",
            line=dict(color='orange')
        ),
        row=2, col=1, secondary_y=False
    )
    
    fig.add_trace(
        go.Scatter(
            x=temporal_df['semana_epidemiologica'],
            y=temporal_df['inmet_precip_tot'],
            name="Precipitação (mm)",
            line=dict(color='blue')
        ),
        row=2, col=1, secondary_y=True
    )
    
    fig.update_xaxes(title_text="Semana Epidemiológica", row=2, col=1)
    fig.update_yaxes(title_text="Casos", row=1, col=1)
    fig.update_yaxes(title_text="Temperatura (°C)", row=2, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Precipitação (mm)", row=2, col=1, secondary_y=True)
    
    fig.update_layout(height=600, showlegend=True)
    
    return fig

@app.callback(
    Output("correlation-heatmap", "figure"),
    Input("uf-filter", "value")
)
def update_correlation_heatmap(selected_uf):
    # Filter data
    filtered_df = df_recent.copy()
    if selected_uf:
        filtered_df = filtered_df[filtered_df['uf'] == selected_uf]
    
    # Calculate correlations
    corr_cols = ['casos_notificados', 'inmet_temp_media', 'inmet_precip_tot',
                 'inmet_temp_media_lag1', 'inmet_precip_tot_lag1',
                 'inmet_temp_media_lag2', 'inmet_precip_tot_lag2']
    
    corr_matrix = filtered_df[corr_cols].corr()
    
    fig = go.Figure(data=go.Heatmap(
        z=corr_matrix.values,
        x=corr_matrix.columns,
        y=corr_matrix.columns,
        colorscale='RdBu',
        zmid=0,
        text=np.round(corr_matrix.values, 2),
        texttemplate="%{text}",
        textfont={"size": 10}
    ))
    
    fig.update_layout(
        title=f"Correlações - {'Brasil' if not selected_uf else selected_uf}",
        height=400
    )
    
    return fig

# CSS styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Eco-Sentinel Dashboard</title>
        {%favicon%}
        {%css%}
        <style>
            .main-container {
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }
            .header {
                text-align: center;
                color: #2c3e50;
                margin-bottom: 30px;
            }
            .dashboard-container {
                display: grid;
                grid-template-columns: 1fr 3fr;
                gap: 20px;
            }
            .filter-section {
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                height: fit-content;
            }
            .alert-section, .prediction-section, .temporal-section, .correlation-section {
                background: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }
            .alert-item {
                padding: 8px;
                margin: 5px 0;
                border-left: 4px solid #ddd;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == '__main__':
    print("🚀 Iniciando Dashboard de Monitoramento...")
    app.run(debug=True, host='0.0.0.0', port=8050)
