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
# CSS - TEMA PROFISSIONAL CLARO
# ============================================================================
st.markdown("""
<style>
    /* === Paleta de Cores Profissional === */
    /* Primary: Teal/Verde-azulado (#0D9488)
       Secondary: Amber/Âmbar (#F59E0B)
       Accent: Rose (#E11D48)
       Background: Slate Gray (#F8FAFC)
       Text: Dark Slate (#1E293B)
    */
    
    /* === Layout Principal === */
    .stApp {
        background-color: #F8FAFC;
        color: #334155;
    }
    
    .main .block-container {
        padding-top: 2rem;
    }
    
    /* === Sidebar === */
    section[data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E2E8F0;
    }
    
    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stMultiSelect label,
    section[data-testid="stSidebar"] .stSlider label,
    section[data-testid="stSidebar"] .stRadio label {
        color: #1E293B !important;
        font-weight: 600;
        font-size: 0.875rem;
    }
    
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #0D9488 !important;
    }
    
    /* === Cards de Métricas === */
    div[data-testid="metric-container"] {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-left: 4px solid #0D9488;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    div[data-testid="metric-container"] label {
        color: #64748B !important;
        font-weight: 600;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.5px;
    }
    
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #0F172A !important;
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    div[data-testid="metric-container"] div[data-testid="stMetricDelta"] {
        font-weight: 600;
    }
    
    /* === Tabs === */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #FFFFFF;
        padding: 8px;
        border-radius: 8px;
        border: 1px solid #E2E8F0;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border-radius: 6px;
        color: #475569;
        font-weight: 600;
        padding: 8px 16px;
        border: none;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: #F1F5F9;
        color: #0D9488;
    }
    
    .stTabs [aria-selected="true"] {
        background: #0D9488 !important;
        color: white !important;
    }
    
    /* === Headers === */
    h1 {
        color: #0F172A !important;
        font-weight: 700;
        font-size: 2rem !important;
    }
    
    h2, h3 {
        color: #1E293B !important;
        font-weight: 600;
    }
    
    /* === Texto Geral === */
    p, span, div, li {
        color: #334155;
    }
    
    /* === Expanders === */
    .streamlit-expanderHeader {
        background-color: #F1F5F9 !important;
        border-radius: 8px;
        font-weight: 600;
        color: #1E293B !important;
    }
    
    .streamlit-expanderContent {
        background-color: #FFFFFF !important;
        color: #334155 !important;
    }

    /* === Inputs e Widgets (Global) === */
    .stSelectbox div[data-baseweb="select"] > div,
    .stMultiSelect div[data-baseweb="select"] > div {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
        border-color: #CBD5E1 !important;
    }
    
    /* Texto dentro do input/select */
    .stSelectbox div[data-baseweb="select"] span,
    .stMultiSelect div[data-baseweb="select"] span {
        color: #0F172A !important;
    }
    
    /* Dropdown menu items */
    li[role="option"] {
        background-color: #FFFFFF !important;
        color: #0F172A !important;
    }
    
    /* Chips/Tags no MultiSelect */
    .stMultiSelect div[data-baseweb="tag"] {
        background-color: #E2E8F0 !important;
        color: #0F172A !important;
    }
    
    /* === Alerts === */
    .stAlert {
        border-radius: 8px;
    }
    
    /* === DataFrames === */
    /* === Menus Dropdown (Correção "Botão List") === */
    div[data-baseweb="popover"],
    div[data-baseweb="menu"],
    ul[role="listbox"] {
        background-color: #FFFFFF !important;
        border: 1px solid #E2E8F0 !important;
    }
    
    li[role="option"]:hover,
    li[role="option"][aria-selected="true"] {
        background-color: #F1F5F9 !important;
        color: #0D9488 !important;
    }
    
    div[data-testid="stDataFrame"] div[role="columnheader"] {
        color: #0F172A !important;
        background-color: #F8FAFC !important;
        border-bottom: 1px solid #E2E8F0 !important;
    }
    
    div[data-testid="stDataFrame"] div[role="gridcell"] {
        color: #334155 !important;
        background-color: #FFFFFF !important;
    }
    
    /* Forçar fundo claro nos expanders e tabelas */
    .stDataFrame, div[data-testid="stDataFrame"] {
        background-color: #FFFFFF !important;
    }
    
    /* Expander fix */
    .streamlit-expanderHeader {
        background-color: #F1F5F9 !important;
        color: #0F172A !important;
        border-radius: 4px;
    }
    
    .streamlit-expanderContent {
        background-color: #FFFFFF !important;
        color: #334155 !important;
        border-top: 1px solid #E2E8F0;
    }

    /* === TABELAS E DATA EDITOR (Correção Fundo Preto) === */
    [data-testid="stDataFrame"], [data-testid="stTable"], .stDataFrame {
        background-color: #FFFFFF !important;
    }
    
    [data-testid="stDataFrame"] div, [data-testid="stTable"] div {
        background-color: #FFFFFF !important;
        color: #0F172A !important; 
    }
    
    /* Header da Tabela */
    [data-testid="stDataFrame"] div[role="columnheader"] {
        color: #0F172A !important;
        background-color: #F1F5F9 !important;
        font-weight: 700 !important;
        border-bottom: 2px solid #CBD5E1 !important;
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

@st.cache_resource
def load_model():
    project_root = get_project_root()
    model_path = project_root / 'models/dengue_model.joblib'
    metadata_path = project_root / 'models/model_metadata.json'
    
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
    
    # Seletor de Modo (UX Preset)
    mode_options = {
        "Visão Geral": "🏠 Visão Geral",
        "Explorador": "🔎 Explorador Avançado",
        "Sazonalidade": "🌦️ Sazonalidade & Clima"
    }
    
    selected_mode_key = st.sidebar.selectbox(
        "Selecione a Análise",
        options=list(mode_options.keys()),
        format_func=lambda x: mode_options[x]
    )
    
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

def render_explorer_mode(df, filters, model, metadata):
    """Modo Explorador: Dashboard Completo"""
    st.header("🔎 Explorador Avançado")
    
    render_kpi_cards(df, filters)
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "📈 Série Temporal",
        "🌡️ Clima",
        "📅 Comparativo",
        "🤖 Modelo ML"
    ])
    
    with tab1:
        render_temporal_analysis(df, filters)
    
    with tab2:
        render_climate_analysis(df)
    
    with tab3:
        render_comparative_analysis(df, filters)
    
    with tab4:
        render_model_info(model, metadata)

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
    elif mode == "Sazonalidade":
        render_seasonal_mode(df_filtered, filters)
    else: # Explorador
        render_explorer_mode(df_filtered, filters, model, metadata)
    
    render_footer()

if __name__ == "__main__":
    main()
