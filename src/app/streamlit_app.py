"""
🦟 Sistema Dengue-Clima - Dashboard
Dashboard para análise de dados epidemiológicos e climáticos.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from pathlib import Path
from datetime import datetime
import joblib
import json
import urllib.request

# ============================================================================
# CONSTANTES GLOBAIS
# ============================================================================

REGIAO_UFS = {
    'Norte': ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO'],
    'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
    'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'],
    'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
    'Sul': ['PR', 'RS', 'SC']
}

def get_regiao_id(uf):
    """Retorna ID numérico da região (0-4) dado a UF"""
    for i, (reg, ufs) in enumerate(REGIAO_UFS.items()):
        if uf in ufs:
            return i
    return -1

@st.cache_data
def load_geojson():
    """Carrega GeoJSON dos estados do Brasil"""
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    try:
        with urllib.request.urlopen(url) as response:
            return json.load(response)
    except Exception as e:
        st.error(f"Erro ao carregar mapa: {e}")
        return None

# ============================================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================================
st.set_page_config(
    page_title="Sistema Dengue-Clima",
    page_icon="🦟",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CSS - TEMA DARK PROFISSIONAL
# ============================================================================
st.markdown("""
<style>
    /* === Paleta Dark Premium === */
    /* Primary: Teal (#0D9488), Secondary: Amber (#F59E0B), Background: Slate (#0E1117) */
    
    /* === Cards de Métricas === */
    div[data-testid="metric-container"] {
        background: #262730;
        border: 1px solid #3D3D4D;
        border-left: 4px solid #0D9488;
        border-radius: 8px;
        padding: 16px;
    }
    
    div[data-testid="metric-container"] label {
        color: #94A3B8 !important;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
    }
    
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #FAFAFA !important;
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    /* === Tabs === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #262730;
        padding: 8px;
        border-radius: 8px;
        border: 1px solid #3D3D4D;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 6px;
        color: #94A3B8;
        font-weight: 600;
        padding: 8px 16px;
        border: none;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: #3D3D4D;
        color: #0D9488;
    }
    
    .stTabs [aria-selected="true"] {
        background: #0D9488 !important;
        color: white !important;
    }
    
    /* === Alerts === */
    .stAlert {
        border-radius: 8px;
    }
    
    /* === Sidebar Headers === */
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #0D9488 !important;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================================
# FUNÇÕES DE CARREGAMENTO
# ============================================================================

def get_project_root():
    current_dir = Path(__file__).resolve().parent
    return current_dir.parent.parent

# Helper para forçar tema claro no Plotly (Alto Contraste)
def apply_light_theme(fig):
    fig.update_layout(
        template='plotly_white',
        paper_bgcolor='rgba(255,255,255,1)',
        plot_bgcolor='rgba(255,255,255,1)',
        font=dict(color='#0F172A', size=12), # Texto quase preto
        xaxis=dict(
            gridcolor='#CBD5E1', # Grid mais visível
            showgrid=True,
            showline=True,
            linecolor='#64748B', # Linha do eixo
            tickfont=dict(color='#0F172A')
        ),
        yaxis=dict(
            gridcolor='#CBD5E1',
            showgrid=True,
            showline=True,
            linecolor='#64748B',
            tickfont=dict(color='#0F172A')
        ),
        legend=dict(
            font=dict(color='#0F172A'),
            bordercolor='#E2E8F0',
            borderwidth=1,
            bgcolor='rgba(255,255,255,0.9)'
        ),
        # Forçar cor escura na barra de cores (Heatmaps/Choropleths)
        coloraxis=dict(
            colorbar=dict(
                title=dict(font=dict(color='#0F172A')),
                tickfont=dict(color='#0F172A'),
                outlinecolor='#CBD5E1',
                outlinewidth=1
            )
        ),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    return fig

@st.cache_data(ttl=3600, show_spinner=False)
def load_data():
    project_root = get_project_root()
    data_path = project_root / 'data/gold/gold_dengue_clima'
    
    if not data_path.exists():
        st.error(f"❌ Diretório de dados não encontrado: {data_path}")
        return pd.DataFrame()
    
    df = pd.DataFrame()
    
    # Tentativa otimizada: ler diretório inteiro (PyArrow)
    try:
        df = pd.read_parquet(data_path)
    except Exception as e:
        print(f"Erro na leitura otimizada: {e}. Tentando método manual...")
        
        parquet_files = list(data_path.rglob("*.parquet"))
        
        if not parquet_files:
            return pd.DataFrame()

        dfs = []
        for f in parquet_files:
            try:
                df_chunk = pd.read_parquet(f)
                if 'uf' not in df_chunk.columns:
                    parts = [p for p in f.parts if p.startswith('uf=')]
                    if parts:
                        df_chunk['uf'] = parts[0].replace('uf=', '')
                dfs.append(df_chunk)
            except Exception as e_chunk:
                print(f"Erro lendo {f}: {e_chunk}")
        
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
    
    if df.empty:
        return df
        
    # === Processamento Pós-Carregamento ===
    
    # Garantir types
    if 'uf' in df.columns:
        # Criar Região se não existir
        if 'regiao' not in df.columns:
            df['regiao'] = df['uf'].apply(lambda x: get_regiao_id(str(x))).astype('int8')
            
        df['uf'] = df['uf'].astype('category')

    if 'nome_municipio' in df.columns:
        df['nome_municipio'] = df['nome_municipio'].astype('category')
            
    # Converter datas
    if 'data_inicio_semana' in df.columns:
        df['data'] = pd.to_datetime(df['data_inicio_semana'])
        df['mes'] = df['data'].dt.month
    else:
        df['mes'] = 1
            
    return df

@st.cache_data(ttl=3600, show_spinner=False)
def load_sanitation_data():
    """Carrega dados combinados de Saneamento e Dengue (Gold)"""
    project_root = get_project_root()
    file_path = project_root / 'data/gold/paineis/painel_municipal_dengue_saneamento.parquet'
    
    if not file_path.exists():
        return pd.DataFrame()
        
    try:
        df = pd.read_parquet(file_path)
        # Enriquecer com Região
        if 'sigla_uf' in df.columns:
            df['regiao'] = df['sigla_uf'].apply(lambda x: get_regiao_id(str(x)))
            # Mapear para nome da região para facilitar plots
            reg_map = {0: 'Norte', 1: 'Nordeste', 2: 'Centro-Oeste', 3: 'Sudeste', 4: 'Sul'}
            df['nome_regiao'] = df['regiao'].map(reg_map)
        return df
    except Exception as e:
        print(f"Erro ao carregar saneamento: {e}")
        return pd.DataFrame()

@st.cache_resource
def load_model():
    project_root = get_project_root()
    # Atualizado para o modelo v3 (Híbrido)
    model_path = project_root / 'models/dengue_hibrido_v3.joblib'
    metadata_path = project_root / 'models/model_metadata_v3.json'
    
    model = None
    metadata = {}
    
    if model_path.exists():
        model = joblib.load(model_path)
    
    if metadata_path.exists():
        with open(metadata_path) as f:
            metadata = json.load(f)
    
    return model, metadata

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def format_number(num, decimals=0):
    if pd.isna(num):
        return "N/A"
    if decimals == 0:
        return f"{int(num):,}".replace(",", ".")
    return f"{num:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")

def calculate_correlation(df, col1, col2):
    valid = df[[col1, col2]].dropna()
    if len(valid) < 3:
        return 0
    return valid[col1].corr(valid[col2])

# ============================================================================
# COMPONENTES DO DASHBOARD
# ============================================================================

def render_header():
    """Header do dashboard"""
    col1, col2 = st.columns([4, 1])
    
    with col1:
        st.title("🦟 Sistema Dengue-Clima")
        st.caption("Análise integrada de dados epidemiológicos e meteorológicos")
    
    with col2:
        st.metric(
            label="📅 Atualização",
            value=datetime.now().strftime("%d/%m/%Y")
        )

def render_sidebar_filters(df):
    """Renderiza filtros laterais com base no modo"""
    st.sidebar.header("🧭 Navegação")
    
    # Seletor de Modo (UX Preset) - Botões Visíveis
    mode_options = ["🏠 Visão Geral", "🔎 Explorador", "🌦️ Sazonalidade", "🚰 Saneamento"]
    mode_keys = ["Visão Geral", "Explorador", "Sazonalidade", "Saneamento"]
    
    selected_mode_display = st.sidebar.radio(
        "📊 Selecione a Análise",
        options=mode_options,
        horizontal=False, # Stack vertical for clarity
        label_visibility="visible"
    )
    
    # Map display to key
    selected_mode_key = mode_keys[mode_options.index(selected_mode_display)]
    
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 Filtros")
    
    # 1. Filtros Temporais (Globais para todos os modos)
    if 'ano_epidemiologico' in df.columns:
        anos_disponiveis = sorted(df['ano_epidemiologico'].dropna().unique().astype(int))
    else:
        anos_disponiveis = [2024]
        
    ano_padrao = anos_disponiveis[-1] if anos_disponiveis else 2024
    
    anos_selecionados = st.sidebar.multiselect(
        "Anos",
        options=anos_disponiveis,
        default=[ano_padrao]
    )
    
    agregacao = st.sidebar.selectbox(
        "Agregação Temporal",
        options=["Semanal", "Mensal", "Anual"],
        index=0
    )
    
    # Inicializa variáveis de filtro local
    selected_regiao = "Todas"
    selected_uf = "Todos"
    selected_municipio = "Todos"
    faixa_casos = None
    faixa_temp = None
    
    # 2. Filtros de Localidade (Apenas no modo Explorador)
    if selected_mode_key == "Explorador":
        st.sidebar.markdown("### 📍 Localidade")
        
        # Região
        regioes_map = {0: 'Norte', 1: 'Nordeste', 2: 'Centro-Oeste', 3: 'Sudeste', 4: 'Sul'}
        regioes_disponiveis = ["Todas"] + list(regioes_map.values())
        
        selected_regiao = st.sidebar.selectbox("Região", options=regioes_disponiveis)
        
        # Filtrar UFs baseado na região
        if selected_regiao != "Todas":
            # Reverse map region name to ID first if needed, or filter DF
            # Simplificação: Filtra DF para pegar UFs da região
            try:
                reg_id = [k for k, v in regioes_map.items() if v == selected_regiao][0]
                ufs_disponiveis = sorted(df[df['regiao'] == reg_id]['uf'].unique())
            except:
                ufs_disponiveis = sorted(df['uf'].dropna().unique())
            ufs_filtradas = ["Todos"] + list(ufs_disponiveis)
        else:
            ufs_filtradas = ["Todos"] + sorted(df['uf'].dropna().unique())
            
        selected_uf = st.sidebar.selectbox("UF", options=ufs_filtradas)
        
        # Filtrar Municípios
        if selected_uf != "Todos":
            municipios_uf = df[df['uf'] == selected_uf]['nome_municipio'].dropna().unique()
            municipios_filtrados = ["Todos"] + sorted(municipios_uf)
        else:
            municipios_filtrados = ["Todos"]
            
        selected_municipio = st.sidebar.selectbox("Município", options=municipios_filtrados)
        
        # Filtros Avançados (Sliders - Opcional)
        st.sidebar.markdown("### 🎚️ Avançado")
        
        # Faixa de Casos
        min_casos = float(df['casos_notificados'].min())
        max_casos = float(df['casos_notificados'].max())
        
        if min_casos < max_casos:
            faixa_casos = st.sidebar.slider(
                "Faixa de Casos",
                min_value=min_casos,
                max_value=max_casos,
                value=(min_casos, max_casos)
            )
            
        # Faixa de Temperatura
        if 'inmet_temp_media' in df.columns:
            min_temp = float(df['inmet_temp_media'].min())
            max_temp = float(df['inmet_temp_media'].max())
            if min_temp < max_temp:
                faixa_temp = st.sidebar.slider(
                    "Temperatura Média (°C)",
                    min_value=min_temp,
                    max_value=max_temp,
                    value=(min_temp, max_temp)
                )

    return selected_mode_key, {
        'anos': anos_selecionados,
        'agregacao': agregacao,
        'regiao': selected_regiao,
        'uf': selected_uf,
        'nome_municipio': selected_municipio,
        'faixa_casos': faixa_casos,
        'faixa_temp': faixa_temp
    }

def apply_filters(df, filters):
    """Aplica filtros"""
    df_filtered = df.copy()
    
    # Mapa de regiões para filtragem inversa
    regiao_map_rev = {
        'Norte': ['AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO'],
        'Nordeste': ['AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE'],
        'Centro-Oeste': ['DF', 'GO', 'MS', 'MT'],
        'Sudeste': ['ES', 'MG', 'RJ', 'SP'],
        'Sul': ['PR', 'RS', 'SC']
    }
    
    if filters['regiao'] != "Todas":
        ufs_regiao = regiao_map_rev.get(filters['regiao'], [])
        df_filtered = df_filtered[df_filtered['uf'].isin(ufs_regiao)]
    
    if filters['uf'] != "Todos":
        df_filtered = df_filtered[df_filtered['uf'] == filters['uf']]
        
    if filters['nome_municipio'] != "Todos":
         df_filtered = df_filtered[df_filtered['nome_municipio'] == filters['nome_municipio']]
    
    # Filtro Temporal (Multiselect de Anos)
    if 'ano_epidemiologico' in df_filtered.columns and filters['anos']:
        df_filtered = df_filtered[df_filtered['ano_epidemiologico'].isin(filters['anos'])]
    
    if filters.get('faixa_casos') and len(df_filtered) > 0:
        min_c, max_c = filters['faixa_casos']
        df_filtered = df_filtered[
            (df_filtered['casos_notificados'] >= min_c) &
            (df_filtered['casos_notificados'] <= max_c)
        ]
    
    if filters.get('faixa_temp') and 'inmet_temp_media' in df_filtered.columns:
        min_t, max_t = filters['faixa_temp']
        mask = (
            (df_filtered['inmet_temp_media'] >= min_t) &
            (df_filtered['inmet_temp_media'] <= max_t)
        ) | df_filtered['inmet_temp_media'].isna()
        df_filtered = df_filtered[mask]
    
    if 'ano_epidemiologico' in df_filtered.columns and 'semana_epidemiologica' in df_filtered.columns:
        df_filtered = df_filtered.sort_values(['ano_epidemiologico', 'semana_epidemiologica'])
    
    return df_filtered

def get_scope_label(filters):
    """Retorna label do escopo"""
    if filters['nome_municipio'] != "Todos":
        return f"{filters['uf']} - {filters['nome_municipio']}"
    elif filters['uf'] != "Todos":
        return f"Estado: {filters['uf']}"
    elif filters['regiao'] != "Todas":
        return f"Região: {filters['regiao']}"
    else:
        return "Brasil (Todos os Estados)"

def render_kpi_cards(df, filters):
    """KPIs"""
    scope_label = get_scope_label(filters)
    st.subheader(f"📊 {scope_label}")
    
    total_casos = df['casos_notificados'].sum()
    media_semanal = df['casos_notificados'].mean()
    max_casos = df['casos_notificados'].max()
    
    temp_media = df['inmet_temp_media'].mean() if 'inmet_temp_media' in df.columns else None
    precip_total = df['inmet_precip_tot'].sum() if 'inmet_precip_tot' in df.columns else None
    
    incidencia_media = df['incidencia_100k'].mean() if 'incidencia_100k' in df.columns else 0
    
    n_municipios = df['geocode'].nunique() if 'geocode' in df.columns else 0
    n_ufs = df['uf'].nunique() if 'uf' in df.columns else 0
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("🦟 Total de Casos", format_number(total_casos))
    
    with col2:
        st.metric("📈 Média Semanal", format_number(media_semanal, 1))
    
    with col3:
        st.metric("🔺 Pico Máximo", format_number(max_casos))
    
    with col4:
        st.metric("👥 Incidência/100k", format_number(incidencia_media, 1))
    
    with col5:
        if filters['uf'] == "Todos":
            st.metric("🏛️ Estados", format_number(n_ufs))
        else:
            st.metric("🏙️ Municípios", format_number(n_municipios))
    
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📅 Semanas de Dados", format_number(len(df)))
    
    with col2:
        if temp_media:
            st.metric("🌡️ Temp. Média", f"{temp_media:.1f}°C")
    
    with col3:
        if precip_total:
            st.metric("🌧️ Precipitação Total", f"{format_number(precip_total, 0)} mm")
    
    with col4:
        if 'inmet_precip_tot' in df.columns:
            corr = calculate_correlation(df, 'casos_notificados', 'inmet_precip_tot')
            st.metric("📊 Correl. Casos×Chuva", f"{corr:.3f}")

def aggregate_data(df, agregacao):
    """Agrega dados"""
    agg_dict = {'casos_notificados': 'sum', 'populacao': 'sum'}
    
    if 'inmet_temp_media' in df.columns:
        agg_dict['inmet_temp_media'] = 'mean'
    if 'inmet_precip_tot' in df.columns:
        agg_dict['inmet_precip_tot'] = 'sum'
    
    if agregacao == "Mensal":
        df_agg = df.groupby(['ano_epidemiologico', 'mes']).agg(agg_dict).reset_index()
        df_agg['periodo'] = df_agg['ano_epidemiologico'].astype(str) + '-' + df_agg['mes'].astype(str).str.zfill(2)
    elif agregacao == "Anual":
        df_agg = df.groupby('ano_epidemiologico').agg(agg_dict).reset_index()
        df_agg['periodo'] = df_agg['ano_epidemiologico'].astype(str)
    else:
        df_agg = df.copy()
        df_agg['periodo'] = df_agg['ano_epidemiologico'].astype(str) + '-S' + df_agg['semana_epidemiologica'].astype(str).str.zfill(2)
    
    return df_agg

def render_temporal_analysis(df, filters):
    """Gráficos temporais"""
    st.subheader("📈 Evolução Temporal")
    
    df_agg = aggregate_data(df, filters['agregacao'])
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        row_heights=[0.7, 0.3],
        subplot_titles=("Casos Notificados", "Variação Percentual")
    )
    
    # Área de casos
    fig.add_trace(
        go.Scatter(
            x=df_agg['periodo'],
            y=df_agg['casos_notificados'],
            mode='lines',
            fill='tozeroy',
            name='Casos',
            line=dict(color='#0D9488', width=2),
            fillcolor='rgba(13, 148, 136, 0.15)'
        ),
        row=1, col=1
    )
    
    # Média móvel
    if len(df_agg) > 4:
        df_agg['media_movel'] = df_agg['casos_notificados'].rolling(window=4, min_periods=1).mean()
        fig.add_trace(
            go.Scatter(
                x=df_agg['periodo'],
                y=df_agg['media_movel'],
                mode='lines',
                name='Média Móvel (4)',
                line=dict(color='#F59E0B', width=3)
            ),
            row=1, col=1
        )
    
    # Variação Percentual (Gráfico de Baras)
    df_agg['variacao_pct'] = df_agg['casos_notificados'].pct_change() * 100
    
    # Cores contrastantes para variação
    colors = ['#10B981' if v < 0 else '#EF4444' for v in df_agg['variacao_pct'].fillna(0)]
    
    fig.add_trace(
        go.Bar(
            x=df_agg['periodo'],
            y=df_agg['variacao_pct'],
            name='Variação %',
            marker_color=colors,
            marker_line_color='#64748B', # Borda nas barras
            marker_line_width=1,
            opacity=0.9 # Menos transparente
        ),
        row=2, col=1
    )
    
    # Layout Ajustado (Zeroline e Grid fortes)
    fig.update_layout(
        height=550,
        template='plotly_white',
        showlegend=True,
        legend=dict(
            orientation='h', 
            yanchor='bottom', y=1.02, 
            xanchor='right', x=1,
            font=dict(color='#0F172A')
        ),
        font=dict(family="Inter, sans-serif", color="#0F172A"),
        # Zeroline preta para destacar o eixo 0
        yaxis2=dict(zeroline=True, zerolinecolor='#334155', zerolinewidth=1.5)
    )
    
    fig = apply_light_theme(fig)
    st.plotly_chart(fig, use_container_width=True)
    
    # Estatísticas
    with st.expander("📊 Estatísticas Detalhadas"):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Distribuição**")
            stats = df['casos_notificados'].describe()
            st.dataframe(
                pd.DataFrame({
                    'Estatística': ['Mínimo', '25%', 'Mediana', '75%', 'Máximo'],
                    'Valor': [format_number(stats['min']), format_number(stats['25%']), 
                             format_number(stats['50%']), format_number(stats['75%']), 
                             format_number(stats['max'])]
                }),
                hide_index=True
            )
        
        with col2:
            st.markdown("**Top 5 Períodos**")
            top5 = df_agg.nlargest(5, 'casos_notificados')[['periodo', 'casos_notificados']]
            top5['casos_notificados'] = top5['casos_notificados'].apply(lambda x: format_number(x))
            top5.columns = ['Período', 'Casos']
            st.dataframe(top5, hide_index=True)
        
        with col3:
            st.markdown("**Tendência**")
            if len(df_agg) > 1:
                tendencia = np.polyfit(range(len(df_agg)), df_agg['casos_notificados'], 1)[0]
                if tendencia > 0:
                    st.error(f"📈 Alta: +{tendencia:.1f} casos/período")
                else:
                    st.success(f"📉 Baixa: {tendencia:.1f} casos/período")

def render_climate_analysis(df):
    """Análise climática"""
    st.subheader("🌡️ Correlação Climática")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Matriz de Correlação**")
        corr_cols = ['casos_notificados']
        for col in ['inmet_temp_media', 'inmet_precip_tot', 'inmet_temp_media_lag1', 'inmet_precip_tot_lag1']:
            if col in df.columns:
                corr_cols.append(col)
        
        if len(corr_cols) > 1:
            corr_matrix = df[corr_cols].corr()
            rename_map = {
                'casos_notificados': 'Casos',
                'inmet_temp_media': 'Temperatura',
                'inmet_precip_tot': 'Precipitação',
                'inmet_temp_media_lag1': 'Temp. (lag 1)',
                'inmet_precip_tot_lag1': 'Precip. (lag 1)'
            }
            corr_matrix = corr_matrix.rename(columns=rename_map, index=rename_map)
            
            fig = px.imshow(
                corr_matrix,
                text_auto='.2f',
                color_continuous_scale='Teal',
                aspect='auto',
                zmin=-1, zmax=1
            )
            fig.update_layout(height=350, template='plotly_white', font=dict(color="#334155"))
            fig = apply_light_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("**Casos × Temperatura**")
        if 'inmet_temp_media' in df.columns:
            fig = px.scatter(
                df.dropna(subset=['inmet_temp_media']),
                x='inmet_temp_media',
                y='casos_notificados',
                trendline='ols',
                opacity=0.4,
                color_discrete_sequence=['#0D9488']
            )
            fig.update_layout(
                xaxis_title="Temperatura Média (°C)",
                yaxis_title="Casos",
                height=350,
                template='plotly_white',
                font=dict(color="#334155")
            )
            fig = apply_light_theme(fig)
            st.plotly_chart(fig, use_container_width=True)
    
    # Série dual axis
    if 'inmet_precip_tot' in df.columns:
        st.markdown("**📊 Casos vs Precipitação ao Longo do Tempo**")
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(
                x=df.index,
                y=df['inmet_precip_tot'],
                name='Precipitação (mm)',
                marker_color='rgba(13, 148, 136, 0.4)'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['casos_notificados'],
                name='Casos',
                line=dict(color='#F59E0B', width=2)
            ),
            secondary_y=True
        )
        
        fig.update_layout(
            height=400,
            template='plotly_white',
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            font=dict(color="#334155")
        )
        fig.update_yaxes(title_text="Precipitação (mm)", secondary_y=False)
        fig.update_yaxes(title_text="Casos", secondary_y=True)
        
        fig = apply_light_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

def render_comparative_analysis(df_original, filters):
    """Comparativo anual"""
    st.subheader("📅 Comparativo por Ano")
    
    df = apply_filters(df_original.copy(), filters)
    
    if 'ano_epidemiologico' in df.columns and 'semana_epidemiologica' in df.columns:
        df_comp = df.groupby(['ano_epidemiologico', 'semana_epidemiologica'])['casos_notificados'].sum().reset_index()
        
        fig = px.line(
            df_comp,
            x='semana_epidemiologica',
            y='casos_notificados',
            color='ano_epidemiologico',
            markers=True,
            color_discrete_sequence=['#0D9488', '#F59E0B', '#EF4444', '#8B5CF6', '#6B7280']
        )
        
        fig.update_layout(
            xaxis_title="Semana Epidemiológica",
            yaxis_title="Casos",
            height=400,
            template='plotly_white',
            legend_title="Ano",
            font=dict(color="#334155")
        )
        
        fig = apply_light_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela resumo
        with st.expander("📋 Resumo por Ano"):
            resumo = df.groupby('ano_epidemiologico').agg({
                'casos_notificados': ['sum', 'mean', 'max']
            }).round(1)
            resumo.columns = ['Total', 'Média/Semana', 'Pico']
            st.dataframe(resumo, use_container_width=True)

def render_model_info(model, metadata):
    """Info do modelo ML"""
    st.subheader("🤖 Modelo Preditivo")
    
    if not model:
        st.info("ℹ️ Modelo não carregado. Execute o treinamento para ativar predições.")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Informações**")
        st.markdown(f"- **Tipo:** {metadata.get('model_type', 'N/A')}")
        st.markdown(f"- **Treinado em:** {metadata.get('trained_at', 'N/A')[:10] if metadata.get('trained_at') else 'N/A'}")
        st.markdown(f"- **Features:** {len(metadata.get('features', []))}")
    
    with col2:
        st.markdown("**Performance**")
        if 'metrics' in metadata and 'test' in metadata['metrics']:
            metrics = metadata['metrics']['test']
            c1, c2, c3 = st.columns(3)
            c1.metric("R²", f"{metrics.get('r2', 0):.4f}")
            c2.metric("MAE", f"{metrics.get('mae', 0):.1f}")
            c3.metric("RMSE", f"{metrics.get('rmse', 0):.1f}")
    
    # Feature importance
    if hasattr(model, 'feature_importances_') and 'features' in metadata:
        st.markdown("**Importância das Features (Top 10)**")
        importance_df = pd.DataFrame({
            'Feature': metadata['features'],
            'Importância': model.feature_importances_
        }).sort_values('Importância', ascending=True).tail(10)
        
        fig = px.bar(
            importance_df,
            x='Importância',
            y='Feature',
            orientation='h',
            color='Importância',
            color_continuous_scale='Teal'
        )
        fig.update_layout(height=350, template='plotly_white', showlegend=False, font=dict(color="#334155"))
        fig = apply_light_theme(fig)
        st.plotly_chart(fig, use_container_width=True)

def render_footer():
    """Footer"""
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**📊 Fontes de Dados**")
        st.caption("InfoDengue | INMET | IBGE")
    
    with col2:
        st.markdown("**💻 Desenvolvido por**")
        st.caption("[Allan Magno](https://github.com/Allanmagnoo)")
    
    with col3:
        st.markdown("**📅 Última Atualização**")
        st.caption(datetime.now().strftime("%d/%m/%Y às %H:%M"))

# ============================================================================
# MODOS DE VISUALIZAÇÃO (PRESETS)
# ============================================================================

def render_overview_mode(df, filters):
    """Modo Visão Geral: KPIs, Mapa e Ranking"""
    st.header("🏠 Visão Geral Nacional")
    
    # KPIs Nacionais
    total_casos = df['casos_notificados'].sum()
    media_semanal = df['casos_notificados'].mean()
    n_municipios = df['geocode'].nunique()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("🦟 Total Casos", format_number(total_casos))
    col2.metric("📈 Média Semanal", format_number(media_semanal, 1))
    col3.metric("🏙️ Municípios Afetados", format_number(n_municipios))
    
    if 'inmet_precip_tot' in df.columns:
        col4.metric("🌧️ Chuva Total (Med)", format_number(df['inmet_precip_tot'].mean(), 1) + " mm")
    
    st.markdown("---")
    
    # === MAPA DE CALOR INTERATIVO (Mapbox Style) ===
    st.subheader("🗺️ Densidade Espacial de Casos (Brasil)")
    
    # Agrupar por UF para o mapa
    df_map = df.groupby('uf')['casos_notificados'].sum().reset_index()
    geojson = load_geojson()
    
    if geojson:
        # Mapa moderno usando Mapbox (sem token, estilo carto-positron)
        fig_map = px.choropleth_mapbox(
            df_map,
            geojson=geojson,
            locations='uf',
            featureidkey="properties.sigla",
            color='casos_notificados',
            color_continuous_scale='Teal', # Combinando com o tema
            mapbox_style="carto-positron",
            zoom=3.5,
            center={"lat": -15.793889, "lon": -47.882778}, # Centro em Brasília
            opacity=0.6,
            labels={'casos_notificados':'Total de Casos'}
        )
        
        fig_map.update_layout(
            height=800, # Altura generosa para visualização detalhada
            margin={"r":0,"t":0,"l":0,"b":0}, # Mapa full-container
            template='plotly_white', 
            font=dict(color="#0F172A")
        )
        
        # Ajuste fino da colorbar
        fig_map = apply_light_theme(fig_map)
        fig_map.update_coloraxes(colorbar_title_text="Casos")
        
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("⚠️ Não foi possível carregar o mapa.")
    
    st.markdown("---")

    # 1. Ranking de Estados (Top 10)
    col_chart1, col_chart2 = st.columns([2, 1])
    
    with col_chart1:
        st.subheader("🏆 Ranking de Casos por Estado")
        ranking = df.groupby('uf')['casos_notificados'].sum().reset_index()
        ranking = ranking.sort_values('casos_notificados', ascending=True).tail(12) # Top 12
        
        fig = px.bar(
            ranking,
            x='casos_notificados',
            y='uf',
            orientation='h',
            text_auto='.2s',
            color='casos_notificados',
            color_continuous_scale='Teal'
        )
        fig.update_layout(height=500, template='plotly_white', font=dict(color="#334155"))
        fig = apply_light_theme(fig)
        st.plotly_chart(fig, use_container_width=True)
        
    with col_chart2:
        st.subheader("🥧 Distribuição Regional")
        # Criar coluna regiao dinamicamente se nao existir (já garantido no load_data, mas safety first)
        if 'regiao' not in df.columns:
            df['regiao'] = df['uf'].apply(lambda x: get_regiao_id(str(x)))
            
        reg_dist = df.groupby('regiao')['casos_notificados'].sum().reset_index()
        reg_map = {0: 'Norte', 1: 'Nordeste', 2: 'Centro-Oeste', 3: 'Sudeste', 4: 'Sul'}
        reg_dist['Região'] = reg_dist['regiao'].map(reg_map)
        
        fig_pie = px.pie(
            reg_dist,
            values='casos_notificados',
            names='Região',
            color_discrete_sequence=px.colors.sequential.Teal
        )
        fig_pie.update_layout(template='plotly_white', font=dict(color="#334155"))
        fig_pie = apply_light_theme(fig_pie)
        st.plotly_chart(fig_pie, use_container_width=True)

    # 2. Evolução Nacional (Comparativo simplificado)
    st.subheader("📅 Tendência Nacional Simplificada")
    render_temporal_analysis(df, {'agregacao': 'Mensal'})

def render_seasonal_mode(df, filters):
    """Modo Sazonalidade: Foco em Clima"""
    st.header("🌦️ Análise Sazonal e Climática")
    
    st.info("Este modo foca exclusivamente nas correlações entre variáveis climáticas (Temperatura/Chuva) e a incidência de casos.")
    
    # === NOVO: Scatter Espacial (Temperatura vs Casos por UF) ===
    st.subheader("🌡️ Geografia do Clima: Calor vs Casos")
    if 'inmet_temp_media' in df.columns:
        # Agrupar por UF (Média de Temp, Soma de Casos, Média de Casos)
        df_geo_clim = df.groupby('uf').agg({
            'inmet_temp_media': 'mean',
            'casos_notificados': 'sum', # Soma total de casos no periodo
            'regiao': 'first' # Para colorir por região
        }).reset_index()
        
        # Mapear região para nome
        reg_map = {0: 'Norte', 1: 'Nordeste', 2: 'Centro-Oeste', 3: 'Sudeste', 4: 'Sul'}
        df_geo_clim['Região'] = df_geo_clim['regiao'].map(reg_map)
        
        fig_scatter = px.scatter(
            df_geo_clim,
            x='inmet_temp_media',
            y='casos_notificados',
            text='uf',
            size='casos_notificados',
            color='Região',
            hover_name='uf',
            title="Relacão: Temperatura Média vs Total de Casos (por Estado)",
            labels={'inmet_temp_media': 'Temperatura Média Geral (°C)', 'casos_notificados': 'Total de Casos'}
        )
        fig_scatter.update_traces(textposition='top center')
        fig_scatter.update_layout(height=500, template='plotly_white', font=dict(color="#334155"))
        fig_scatter = apply_light_theme(fig_scatter)
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    st.markdown("---")
    
    # Reusa a função de clima existente (Detalhada Temporal)
    render_climate_analysis(df)
    
    st.markdown("---")
    st.subheader("🔍 Lags e Efeitos Tardios")
    if 'casos_lag1' in df.columns:
        # Correlação com lags
        lags = ['casos_lag1', 'casos_lag2', 'casos_lag3', 'casos_lag4']
        valid_lags = [l for l in lags if l in df.columns]
        if valid_lags:
            corr_data = df[['casos_notificados'] + valid_lags].corr()['casos_notificados'].drop('casos_notificados')
            
            fig_lag = px.bar(
                x=corr_data.index,
                y=corr_data.values,
                labels={'x': 'Lag (Semanas)', 'y': 'Correlação'},
                title="Autocorrelação de Casos (Persistência)"
            )
            fig_lag.update_layout(template='plotly_white', font=dict(color="#334155"))
            fig_lag = apply_light_theme(fig_lag)
            st.plotly_chart(fig_lag, use_container_width=True)

def render_sanitation_mode(df_san):
    """Renderiza a visão de Saneamento"""
    st.subheader("🚰 Saneamento Básico & Dengue")
    st.info("ℹ️ Análise cruzada entre indicadores do SNIS (Sistema Nacional de Informações sobre Saneamento) e incidência de Dengue.")
    
    if df_san.empty:
        st.error("Dados de saneamento não disponíveis.")
        return

    # Filtros Básicos
    regioes = ["Todas"] + sorted(df_san['nome_regiao'].dropna().unique())
    sel_regiao = st.selectbox("Filtrar Região", regioes)
    
    df_plot = df_san.copy()
    if sel_regiao != "Todas":
        df_plot = df_plot[df_plot['nome_regiao'] == sel_regiao]
        
    # KPIs Rápidos
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🏙️ Municípios", len(df_plot))
    with col2:
        cob_esgoto = df_plot['esgoto_cobertura_total_pct'].mean()
        st.metric("🚽 Cobertura Esgoto (Média)", f"{cob_esgoto:.1f}%")
    with col3:
        perda_agua = df_plot['agua_perda_distribuicao_pct'].mean()
        st.metric("💧 Perda de Água (Média)", f"{perda_agua:.1f}%")
    with col4:
        incidencia = df_plot['incidencia_dengue_100k'].median()
        st.metric("🦟 Incidência Mediana", f"{incidencia:.0f}/100k")
        
    st.markdown("---")
    
    col_a, col_b = st.columns(2)
    
    # 1. Scatter: Esgoto vs Dengue
    with col_a:
        st.markdown("#### 🚽 Esgoto vs Incidência")
        fig1 = px.scatter(
            df_plot,
            x='esgoto_cobertura_total_pct',
            y='incidencia_dengue_100k',
            color='nome_regiao',
            hover_name='id_municipio',
            title="Cobertura de Esgoto x Incidência de Dengue",
            labels={'esgoto_cobertura_total_pct': 'Cobertura Esgoto (%)', 'incidencia_dengue_100k': 'Incidência (casos/100k)'},
            height=400,
            opacity=0.6,
            log_y=True,
            range_x=[0, 100] # Fixar eixo X de 0 a 100%
        )
        fig1.update_layout(template='plotly_dark')
        st.plotly_chart(fig1, use_container_width=True)
        st.caption("Nota: Escala Y logarítmica. Municípios com MENOS esgoto tendem a ter MAIS casos?")

    # 2. Scatter: Perda de Água vs Dengue
    with col_b:
        st.markdown("#### 💧 Perda de Água vs Incidência")
        fig2 = px.scatter(
            df_plot,
            x='agua_perda_distribuicao_pct',
            y='incidencia_dengue_100k',
            color='nome_regiao',
            hover_name='id_municipio',
            title="Perda de Água x Incidência de Dengue",
            labels={'agua_perda_distribuicao_pct': 'Perda na Distribuição (%)', 'incidencia_dengue_100k': 'Incidência'},
            height=400,
            opacity=0.6,
            log_y=True,
            range_x=[0, 100] # Fixar eixo X de 0 a 100%
        )
        fig2.update_layout(template='plotly_dark')
        st.plotly_chart(fig2, use_container_width=True)
        st.caption("Água perdida gera poças? Veja a correlação.")
        
    st.markdown("---")
    
    # 3. Bar Chart Agregado: Incidência por Faixa de Cobertura
    st.markdown("#### 📊 Incidência Média por Faixa de Saneamento")
    
    col_c, col_d = st.columns(2)
    
    with col_c:
        # Binning Esgoto
        bins = [0, 20, 50, 80, 100]
        labels = ['Crítico (0-20%)', 'Baixo (20-50%)', 'Médio (50-80%)', 'Alto (>80%)']
        df_plot['faixa_esgoto'] = pd.cut(df_plot['esgoto_cobertura_total_pct'], bins=bins, labels=labels)
        
        df_bar = df_plot.groupby('faixa_esgoto')['incidencia_dengue_100k'].mean().reset_index()
        
        fig3 = px.bar(
            df_bar,
            x='faixa_esgoto',
            y='incidencia_dengue_100k',
            color='faixa_esgoto',
            color_discrete_sequence=px.colors.qualitative.Prism,
            title="Por Cobertura de Esgoto",
            labels={'incidencia_dengue_100k': 'Incidência Média', 'faixa_esgoto': 'Faixa'}
        )
        fig3.update_layout(showlegend=False, template='plotly_dark')
        st.plotly_chart(fig3, use_container_width=True)
        
    with col_d:
        # Binning Densidade (IBGE)
        if 'densidade_domiciliada' in df_plot.columns:
            # Quartis para densidade
            try:
                df_plot['faixa_densidade'] = pd.qcut(df_plot['densidade_domiciliada'], q=4, labels=['Baixa', 'Média', 'Alta', 'Muito Alta'])
                df_bar_dens = df_plot.groupby('faixa_densidade')['incidencia_dengue_100k'].mean().reset_index()
                
                fig4 = px.bar(
                    df_bar_dens,
                    x='faixa_densidade',
                    y='incidencia_dengue_100k',
                    color='faixa_densidade',
                    color_discrete_sequence=px.colors.sequential.Teal,
                    title="Por Densidade Domiciliada (IBGE)",
                    labels={'incidencia_dengue_100k': 'Incidência Média', 'faixa_densidade': 'Densidade'}
                )
                fig4.update_layout(showlegend=False, template='plotly_dark')
                st.plotly_chart(fig4, use_container_width=True)
            except Exception as e:
                st.info(f"Não foi possível gerar gráfico de densidade: {e}")
    
    st.markdown("---")
    
    # === 4. MAPA CHOROPLETH: Cobertura de Esgoto por UF ===
    st.markdown("#### 🗺️ Mapa de Cobertura de Esgoto por Estado")
    
    # Agregar por UF
    df_uf = df_san.groupby('sigla_uf').agg({
        'esgoto_cobertura_total_pct': 'mean',
        'incidencia_dengue_100k': 'mean'
    }).reset_index()
    
    geojson = load_geojson()
    
    if geojson:
        fig_map = px.choropleth_mapbox(
            df_uf,
            geojson=geojson,
            locations='sigla_uf',
            featureidkey="properties.sigla",
            color='esgoto_cobertura_total_pct',
            color_continuous_scale='RdYlGn', # Vermelho (ruim) -> Amarelo -> Verde (bom)
            mapbox_style="carto-darkmatter", # Dark theme
            zoom=3.5,
            center={"lat": -15.793889, "lon": -47.882778},
            opacity=0.7,
            labels={'esgoto_cobertura_total_pct': 'Cobertura Esgoto (%)'}
        )
        fig_map.update_layout(
            height=600,
            margin={"r":0,"t":0,"l":0,"b":0},
            template='plotly_dark'
        )
        st.plotly_chart(fig_map, use_container_width=True)
        st.caption("Verde = Alta cobertura de esgoto. Vermelho = Baixa cobertura.")
    else:
        st.warning("Mapa não disponível.")
    
    st.markdown("---")
    
    # === 5. MATRIZ DE CORRELAÇÃO (Heatmap) ===
    st.markdown("#### 🔥 Matriz de Correlação: Saneamento x Dengue")
    
    # Selecionar colunas relevantes
    corr_cols = [
        'incidencia_dengue_100k',
        'esgoto_cobertura_total_pct',
        'agua_cobertura_total_pct',
        'agua_perda_distribuicao_pct',
        'densidade_demografica_geral'
    ]
    
    # Filtrar colunas existentes
    corr_cols_valid = [c for c in corr_cols if c in df_san.columns]
    
    if len(corr_cols_valid) > 1:
        corr_matrix = df_san[corr_cols_valid].corr()
        
        # Nomes mais amigáveis
        rename_map = {
            'incidencia_dengue_100k': 'Incidência Dengue',
            'esgoto_cobertura_total_pct': 'Cobertura Esgoto',
            'agua_cobertura_total_pct': 'Cobertura Água',
            'agua_perda_distribuicao_pct': 'Perda Água',
            'densidade_demografica_geral': 'Densidade Demográfica'
        }
        corr_matrix.rename(index=rename_map, columns=rename_map, inplace=True)
        
        fig_corr = px.imshow(
            corr_matrix,
            text_auto='.2f',
            color_continuous_scale='RdBu_r', # Vermelho = Correlação Positiva, Azul = Negativa
            title="Correlação entre Indicadores",
            aspect='auto'
        )
        fig_corr.update_layout(height=450, template='plotly_dark')
        st.plotly_chart(fig_corr, use_container_width=True)
        st.caption("Valores próximos de -1 (azul) = correlação negativa forte. +1 (vermelho) = positiva forte.")
    else:
        st.info("Colunas insuficientes para gerar a matriz de correlação.")

def render_simulation_tab(model, metadata):
    """Simulador Interativo"""
    st.subheader("🔮 Simulador de Cenários (Modelo Híbrido v3)")
    
    if not model:
        st.warning("⚠️ Modelo v3 não carregado.")
        render_model_info(model, metadata)
        return

    st.markdown("""
    Este simulador utiliza o **Modelo XGBoost Híbrido v3** para prever a incidência semanal de dengue
    com base em variáveis epidemiológicas, estruturais e climáticas.
    """)
    
    # --- Inputs ---
    col_input1, col_input2 = st.columns(2)
    
    with col_input1:
        st.markdown("##### 🏥 Epidemiologia (Status Atual)")
        casos_lag1 = st.number_input("Casos Semana Anterior", min_value=0, value=50, help="Quantos casos ocorreram na semana passada?")
        casos_lag2 = st.number_input("Casos Há 2 Semanas", min_value=0, value=45)
        casos_media_4sem = st.number_input("Média Móvel (4 semanas)", min_value=0, value=48)
        
        st.markdown("##### 📅 Sazonalidade")
        semana_epi = st.slider("Semana Epidemiológica", 1, 53, 10)
    
    with col_input2:
        st.markdown("##### 🏗️ Infraestrutura (Cenário)")
        densidade = st.slider("Densidade Domiciliada (dom/km²)", 0.0, 3000.0, 150.0, help="Densidade urbana (IBGE)")
        agua_perda = st.slider("Perda de Água na Distribuição (%)", 0.0, 100.0, 35.0, help="Indicador de ineficiência (SNIS)")
        esgoto_cob = st.slider("Cobertura de Esgoto (%)", 0.0, 100.0, 60.0, help="Acesso a saneamento (SNIS)")
        agua_cob = st.slider("Cobertura de Água (%)", 0.0, 100.0, 95.0)

    # --- Prediction Logic ---
    if st.button("🚀 Simular Previsão", type="primary"):
        try:
            # Create a single row DataFrame
            features = metadata.get('features', [])
            input_data = pd.DataFrame(index=[0], columns=features)
            input_data.fillna(0, inplace=True) # Default 0 for others
            
            # Fill mapped inputs
            input_data['casos_lag1'] = casos_lag1
            input_data['casos_lag2'] = casos_lag2
            # Assuming linear trend for 3 and 4 if not provided
            input_data['casos_lag3'] = casos_lag2 
            input_data['casos_lag4'] = casos_lag2
            input_data['casos_media_4sem'] = casos_media_4sem
            
            input_data['densidade_domiciliada'] = densidade
            input_data['agua_perda_distribuicao_pct'] = agua_perda
            input_data['esgoto_cobertura_total_pct'] = esgoto_cob
            input_data['agua_cobertura_total_pct'] = agua_cob
            
            # Seasonality
            input_data['semana_sin'] = np.sin(2 * np.pi * semana_epi / 53)
            input_data['semana_cos'] = np.cos(2 * np.pi * semana_epi / 53)
            input_data['ano_epidemiologico'] = 2024 # Fixed for sim
            
            # Predict
            pred = model.predict(input_data)[0]
            pred = max(0, pred) # No negative cases
            
            st.divider()
            c_res1, c_res2 = st.columns(2)
            c_res1.metric("🔮 Incidência Prevista", f"{pred:.1f} / 100k hab")
            
            # Context
            risk_level = "Baixo"
            color = "green"
            if pred > 100: 
                risk_level = "Médio" 
                color = "orange"
            if pred > 300: 
                risk_level = "Alto (Epidêmico)" 
                color = "red"
                
            c_res2.markdown(f"**Risco Estimado:** :{color}[{risk_level}]")
            
        except Exception as e:
            st.error(f"Erro na simulação: {e}")
            
    st.divider()
    with st.expander("ℹ️ Detalhes Técnicos do Modelo"):
        render_model_info(model, metadata)

def render_explorer_mode(df, filters, model, metadata):
    """Modo Explorador: Dashboard Completo"""
    st.header("🔎 Explorador Avançado")
    
    render_kpi_cards(df, filters)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Série Temporal",
        "🌡️ Clima",
        "📅 Comparativo",
        "🔮 Simulação IA"
    ])
    
    with tab1:
        render_temporal_analysis(df, filters)
    
    with tab2:
        render_climate_analysis(df)
    
    with tab3:
        render_comparative_analysis(df, filters)
    
    with tab4:
        render_simulation_tab(model, metadata)

# ============================================================================
# MAIN
# ============================================================================

def main():
    with st.spinner('🔄 Carregando dados...'):
        df = load_data()
        model, metadata = load_model()
    
    if df.empty:
        st.error("❌ Não foi possível carregar os dados. Verifique a execução do pipeline Gold.")
        return
    
    render_header()
    
    # Filtros retornam o modo e o dicionário de filtros
    mode, filters = render_sidebar_filters(df)
    
    # Aplica filtros ao DataFrame
    df_filtered = apply_filters(df, filters)
    
    if df_filtered.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros selecionados.")
        return
    
    # Dispatcher de Modos
    if mode == "Visão Geral":
        render_overview_mode(df_filtered, filters)
    elif mode == "Saneamento":
        df_san = load_sanitation_data()
        render_sanitation_mode(df_san)
    elif mode == "Sazonalidade":
        render_seasonal_mode(df_filtered, filters)
    else: # Explorador
        render_explorer_mode(df_filtered, filters, model, metadata)
    
    render_footer()

if __name__ == "__main__":
    main()
