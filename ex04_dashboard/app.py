"""
Ce script lance un dashboard Streamlit qui est connecté a PostgreeSQL (local ou Docker) 
pour visualiser et comparer les données de taxi de New York pour les mois de juin et décembre.
Il propose 3 modes : 
    - Comparaison Globale : 
    - Focus Juin : 
    - Focus Décembre :
avec l'aide des KPI (Key Performance Indicators) et des graphiques interactifs (barres, camembert) pour analyser les tendancess et les performances des vendeurs 
"""

import streamlit as st
import pandas as pd, os
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go

# --- CONFIGURATION ---
st.set_page_config(page_title="NYC Taxi | Comparateur Gold", page_icon="🚖", layout="wide")

# CSS pour un look "Premium"
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; color: #FFC107; }
    .main { background-color: #f8f9fa; }
    </style>
    """, unsafe_allow_html=True)

IS_DOCKER = os.path.exists('/.dockerenv')
DB_HOST = "postgres" if IS_DOCKER else "localhost"

# Utilisation de la variable DB_HOST dans l'URL
engine = create_engine(f"postgresql://postgres:postgres@{DB_HOST}:5432/postgres")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🚖 Dashboard Gold")
    st.markdown("---")
    mode = st.radio("Mode d'affichage", ["Comparaison Globale", "Focus Juin", "Focus Décembre"])
    st.markdown("---")
    st.info("Données certifiées issues du pipeline Spark")

# --- LOGIQUE DE FILTRAGE & REQUÊTES ---
if mode == "Comparaison Globale":
    st.title("📊 Comparatif : Juin vs Décembre")

    # Requête pour comparer les deux mois côte à côte
    query = """
            SELECT
                trip_month,
                COUNT(*) as nb_trajets,
                SUM(total_amount) as CA,
                AVG(trip_distance) as dist_moy,
                AVG(fare_amount) as prix_course
            FROM fact_trips
            GROUP BY trip_month \
            """
    df_comp = pd.read_sql(query, engine)
    df_comp['Mois'] = df_comp['trip_month'].map({6: 'Juin', 12: 'Décembre'})

    # --- SECTION KPI COMPARATIFS ---
    c1, c2, c3 = st.columns(3)
    # Calcul des écarts (Delta)
    rev_juin = df_comp[df_comp['trip_month']==6]['ca'].values[0]
    rev_dec = df_comp[df_comp['trip_month']==12]['ca'].values[0]
    delta_rev = ((rev_dec - rev_juin) / rev_juin) * 100

    c1.metric("Volume Total (7M+)", f"{df_comp['nb_trajets'].sum():,}")
    c2.metric("Chiffre d'Affaires Total", f"{df_comp['ca'].sum():,.0f} $", f"{delta_rev:.1f}% vs Juin")
    c3.metric("Moyenne Distance", f"{df_comp['dist_moy'].mean():.2f} mi")

    # --- GRAPHIQUES DE COMPARAISON ---
    st.markdown("### 📈 Analyse Comparative")
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Volume de courses par mois")
        fig_vol = px.bar(df_comp, x='Mois', y='nb_trajets', color='Mois',
                         color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'},
                         text_auto='.2s')
        st.plotly_chart(fig_vol, width='stretch')

    with col_right:
        st.subheader("Répartition du CA (Market Share)")
        fig_pie = px.pie(df_comp, values='ca', names='Mois', hole=0.5,
                         color_discrete_sequence=['#90CAF9', '#1565C0'])
        st.plotly_chart(fig_pie, width='stretch')

    # --- TENDANCE TEMPORELLE CROISÉE ---
    st.subheader("🕒 Superposition des tendances journalières")
    query_line = """
                 SELECT EXTRACT(DAY FROM pickup_datetime) as jour, trip_month, SUM(total_amount) as revenue
                 FROM fact_trips
                 GROUP BY jour, trip_month ORDER BY jour \
                 """
    df_line = pd.read_sql(query_line, engine)
    df_line['Mois'] = df_line['trip_month'].map({6: 'Juin', 12: 'Décembre'})
    fig_line = px.line(df_line, x='jour', y='revenue', color='Mois',
                       title="CA par jour du mois (Juin vs Décembre)",
                       labels={'jour': 'Jour du mois'},
                       color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})
    st.plotly_chart(fig_line, width='stretch')

    # --- FOOTER GOUVERNANCE (Toujours visible) ---
    st.markdown("---")
    with st.expander("🛡️ Voir les métriques de Gouvernance (Architecture Medallion)"):
        total_raw = 7087351 #
        total_gold = pd.read_sql("SELECT COUNT(*) FROM fact_trips", engine).iloc[0,0]

        st.write(f"**Bronze Layer (Brut)** : {total_raw:,} lignes")
        st.write(f"**Gold Layer (Nettoyé)** : {total_gold:,} lignes")
        st.progress(total_gold / total_raw)
        st.write(f"✨ Qualité des données : { (total_gold/total_raw)*100:.2f}% des données ont été validées par Spark.")

else:
    
    # --- MODE FOCUS (JUIN OU DÉCEMBRE) ---
    selected_month = 6 if mode == "Focus Juin" else 12
    month_name = "Juin" if selected_month == 6 else "Décembre"
    st.title(f"🔍 Focus Exclusif : {month_name}")

    # 1. KPI Uniques pour le mois
    query_kpi = f"SELECT COUNT(*) as n, SUM(total_amount) as r, AVG(trip_distance) as d FROM fact_trips WHERE trip_month = {selected_month}"
    kpis = pd.read_sql(query_kpi, engine).iloc[0]

    f1, f2, f3 = st.columns(3)
    f1.metric("Trajets", f"{int(kpis['n']):,}")
    f2.metric("Chiffre d'Affaires", f"{kpis['r']:,.0f} $")
    f3.metric("Distance Moyenne", f"{kpis['d']:.2f} mi")

    st.markdown("---")

    # 2. Top Zones & Vendeurs spécifiques au mois
    col_x, col_y = st.columns(2)

    with col_x:
        st.subheader(f"Top 10 Destination en {month_name}")
        q_zones = f"""
            SELECT z.zone, COUNT(*) as volume 
            FROM fact_trips f JOIN dim_zone z ON f.dropoff_location_id = z.location_id
            WHERE f.trip_month = {selected_month}
            GROUP BY z.zone ORDER BY volume DESC LIMIT 10
        """
        df_z = pd.read_sql(q_zones, engine)
        st.plotly_chart(px.bar(df_z, x='volume', y='zone', orientation='h', color='volume'), width='stretch')

    with col_y:
        st.subheader(f"Performance Vendeurs ({month_name})")
        q_vend = f"""
            SELECT v.vendor_name, SUM(f.total_amount) as rev 
            FROM fact_trips f JOIN dim_vendor v ON f.vendor_id = v.vendor_id
            WHERE f.trip_month = {selected_month}
            GROUP BY v.vendor_name
        """
        df_v = pd.read_sql(q_vend, engine)
        st.plotly_chart(px.bar(df_v, x='vendor_name', y='rev', color='vendor_name'), width='stretch')
