import streamlit as st
import pandas as pd
import joblib
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
st.cache_resource.clear()
# --- CONFIGURATION ---
st.set_page_config(page_title="NYC Taxi | ML & Dashboard", page_icon="🚖", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 28px; color: #FFC107; }
    .main { background-color: #f8f9fa; }
    </style>
    """, unsafe_allow_html=True)

@st.cache_resource
def get_engine():
    return create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

@st.cache_resource
def load_ml_model():
    # Charge le modèle de l'exercice 5
    return joblib.load("/opt/project/ex05_ml_prediction_service/taxi_model.joblib")

engine = get_engine()
model = load_ml_model()
rmse_val = getattr(model, 'rmse_score', "Non défini")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🚖 Dashboard & Prédiction")
    st.markdown("---")
    # Ajout de l'option ML dans ton menu radio d'origine
    mode = st.radio("Navigation", ["Comparaison Globale", "Focus Juin", "Focus Décembre", "🤖 Prédiction de Prix"])
    st.markdown("---")

if mode == "🤖 Prédiction de Prix":
    st.title("🤖 Estimateur de Prix Intelligent")
    st.markdown("""
        Cette interface utilise un modèle **Random Forest** pour prédire le coût total d'une course.
        Les prédictions incluent le tarif de base, les suppléments de temps et les taxes estimées.
    """)

    # --- ZONE DE SAISIE ---
    with st.container(border=True):
        col1, col2 = st.columns([1, 1])

        with col1:
            st.subheader("📍 Détails du trajet")
            # Récupération des zones
            zones_df = pd.read_sql("SELECT location_id, zone FROM dim_zone ORDER BY zone", engine)

            pick_zone = st.selectbox("Lieu de départ", zones_df['zone'], index=131) # JFK par défaut
            drop_zone = st.selectbox("Destination", zones_df['zone'], index=235) # Times Square par défaut

            pick_id = zones_df[zones_df['zone'] == pick_zone]['location_id'].values[0]
            drop_id = zones_df[zones_df['zone'] == drop_zone]['location_id'].values[0]

            # --- CALCUL AUTO DE LA DISTANCE ---
            query_dist = f"""
                SELECT AVG(trip_distance) as avg_dist
                FROM fact_trips
                WHERE pickup_location_id = {pick_id} AND dropoff_location_id = {drop_id}
            """
            res_dist = pd.read_sql(query_dist, engine).iloc[0,0]

            # Distance par défaut si le trajet est inconnu (moyenne globale de NYC)
            dist = float(res_dist) if res_dist else 2.5

            st.info(f"📏 Distance historique constatée : **{dist:.2f} miles**")

        with col2:
            st.subheader("⏰ Date & Heure")
            h_col, d_col = st.columns(2)
            hour = h_col.slider("Heure de départ", 0, 23, 12)
            day_label = d_col.selectbox("Jour de la semaine", ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"])

        st.markdown("---")
        # Bouton stylisé sur toute la largeur
        predict_btn = st.button("✨ GÉNÉRER L'ESTIMATION", width='stretch', type="primary")

    # --- ZONE DE RÉSULTAT ---
    if predict_btn:
        day_map = {"Lundi":0, "Mardi":1, "Mercredi":2, "Jeudi":3, "Vendredi":4, "Samedi":5, "Dimanche":6}
        pick_id = zones_df[zones_df['zone'] == pick_zone]['location_id'].values[0]
        drop_id = zones_df[zones_df['zone'] == drop_zone]['location_id'].values[0]

        # Préparation des données (Ordre strict de train.py : distance, pick_id, drop_id, hour, dow)
        test_df = pd.DataFrame([[dist, pick_id, drop_id, hour, day_map[day_label]]],
                               columns=['trip_distance', 'pickup_location_id', 'dropoff_location_id', 'pickup_hour', 'day_of_week'])

        prediction = model.predict(test_df)[0]

        # Affichage avec colonnes
        res_col1, res_col2 = st.columns([1, 2])

        with res_col1:
            st.write("")
            st.write("")
            st.metric("Prix Estimé", f"{prediction:.2f} $")
            st.success(f"Estimation basée sur un trajet de {dist} miles vers {drop_zone}.")

            with st.expander("ℹ️ Détails techniques"):
                st.json({
                    "Modèle": "Random Forest Regressor",
                    "Input_features": [dist, pick_id, drop_id, hour, day_map[day_label]],
                    "RMSE_Validation":f"{rmse_val}"
                })

        with res_col2:
            # Jauge dynamique
            max_gauge = max(100, int(prediction * 1.5))
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = prediction,
                number = {'suffix': " $", 'font': {'size': 50}, 'valueformat':'.2f'},
                domain = {'x': [0, 1], 'y': [0, 1]},
                gauge = {
                    'axis': {'range': [0, max_gauge], 'tickwidth': 1, 'tickcolor': "darkblue"},
                    'bar': {'color': "#FFC107"},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, prediction*0.8], 'color': '#e8f5e9'},
                        {'range': [prediction*0.8, prediction*1.2], 'color': '#fff3e0'},
                        {'range': [prediction*1.2, max_gauge], 'color': '#ffebee'}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': prediction
                    }
                }
            ))
            fig_gauge.update_layout(height=300, margin=dict(t=0, b=0, l=10, r=10))
            st.plotly_chart(fig_gauge, width='stretch')

elif mode == "Comparaison Globale":
    st.title("📊 Comparatif Stratégique : Juin vs Décembre")

    # Requête SQL riche (CA, Volume, Efficacité)

    query = """
            SELECT
                trip_month,
                COUNT(*) as nb_trajets,
                SUM(total_amount) as ca,
                AVG(trip_distance) as dist_moy,
                SUM(total_amount) / NULLIF(SUM(trip_distance), 0) as prix_au_mile,
                AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 as tip_pct
            FROM fact_trips
            GROUP BY trip_month \
            """
    df_comp = pd.read_sql(query, engine)
    df_comp['Mois'] = df_comp['trip_month'].map({6: 'Juin', 12: 'Décembre'})

    # 1. KPIs de synthèse
    m1, m2, m3, m4 = st.columns(4)
    rev_juin = df_comp[df_comp['trip_month']==6]['ca'].values[0]
    rev_dec = df_comp[df_comp['trip_month']==12]['ca'].values[0]

    m1.metric("Volume Total (7M+)", f"{df_comp['nb_trajets'].sum():,}")
    m2.metric("CA Total", f"{df_comp['ca'].sum():,.0f} $")
    m3.metric("Efficacité ($/mi)", f"{df_comp['prix_au_mile'].mean():.2f} $")
    m4.metric(
        "Croissance CA",
        f"{((rev_dec - rev_juin) / rev_juin) * 100:+.1f}%",
        help="Évolution du Chiffre d'Affaires de Décembre par rapport à Juin"
    )

    st.markdown("---")

    # 2. Volume vs Chiffre d'Affaires (Côte à côte)
    st.subheader("📈 Analyse des Volumes et Revenus")
    col_left, col_right = st.columns(2)

    with col_left:
        # Ton graphique d'origine : Volume par mois
        fig_vol = px.bar(df_comp, x='Mois', y='nb_trajets', color='Mois',
                         title="Nombre total de trajets",
                         text_auto='.2s',
                         color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})
        st.plotly_chart(fig_vol, width='stretch')

    with col_right:
        st.subheader("💰 Répartition du CA")
        # Création du camembert avec affichage des pourcentages et des valeurs brutes
        fig_pie = px.pie(df_comp,
                         values='ca',
                         names='Mois',
                         hole=0.4, # Aspect "Donut" pour plus de modernité
                         color='Mois',
                         color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})

        # Configuration pour afficher le texte (Valeur + Pourcentage) à l'intérieur ou à l'extérieur
        fig_pie.update_traces(textposition='inside', textinfo='percent+value')

        st.plotly_chart(fig_pie, width='stretch')

    st.markdown("---")

    # 3. Évolution temporelle (Superposition sur toute la largeur)
    st.subheader("🕒 Tendances journalières (Revenu)")
    query_line = "SELECT EXTRACT(DAY FROM pickup_datetime) as jour, trip_month, SUM(total_amount) as revenue FROM fact_trips GROUP BY jour, trip_month ORDER BY jour"
    df_line = pd.read_sql(query_line, engine)
    df_line['Mois'] = df_line['trip_month'].map({6: 'Juin', 12: 'Décembre'})

    fig_line = px.line(df_line, x='jour', y='revenue', color='Mois',
                       template="plotly_white",
                       color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})

    # On déplace la légende en haut pour gagner de la place
    fig_line.update_layout(legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig_line, width='stretch')

    # --- SECTION INSIGHTS COMPORTEMENTAUX (A & B) ---
    st.markdown("---")
    st.subheader("💡 Analyses Comportementales : Saisonnalité")
    col_a, col_b = st.columns(2)

    with col_a:
        st.write("**Générosité (Pourboires)**")
        fig_tip = px.bar(df_comp, x='Mois', y='tip_pct',
                         color='Mois', text_auto='.1f',
                         title="% Moyen du Pourboire / Prix course",
                         color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})

        # CORRECTION ICI : Utilisation de ticksuffix dans le dictionnaire yaxis
        fig_tip.update_layout(yaxis=dict(ticksuffix=" %"), showlegend=False)

        st.plotly_chart(fig_tip, width='stretch')
        st.info("NYC Insight : Une hausse en décembre confirme souvent l'effet 'Holiday Season' sur les pourboires.")

    with col_b:
        st.write("**Distance des trajets**")
        fig_dist = px.bar(df_comp, x='Mois', y='dist_moy',
                          color='Mois', text_auto='.2f',
                          title="Distance moyenne par course (miles)",
                          color_discrete_map={'Juin':'#90CAF9', 'Décembre':'#1565C0'})

        fig_dist.update_layout(showlegend=False)
        st.plotly_chart(fig_dist, width='stretch')
        st.info("Note : Des trajets plus courts en hiver indiquent souvent l'utilisation du taxi comme 'abri' contre le froid.")

    # --- FOOTER GOUVERNANCE ---
    st.markdown("---")
    with st.expander("🛡️ Audit de Santé des Données (Pipeline ETL)"):
        # 1. Récupération des métriques
        total_raw = 7087351  # Ton volume fixe du fichier CSV
        total_gold = pd.read_sql("SELECT COUNT(*) FROM fact_trips", engine).iloc[0,0]
        lignes_rejetees = total_raw - total_gold
        qualite_score = (total_gold / total_raw) * 100

        # 2. Affichage des KPIs de Gouvernance
        g1, g2, g3 = st.columns(3)

        g1.metric("Données Source (Bronze)", f"{total_raw:,}", help="Nombre de lignes brutes injectées")
        g2.metric("Données Nettoyées (Gold)", f"{total_gold:,}", f"{-(total_raw-total_gold):,} filtrées")
        g3.metric("Score de Qualité", f"{qualite_score:.2f} %", help="Ratio de données valides après règles métier")

        # 3. Visualisation du Flux (Barre de progression stylisée)
        st.write(f"**Flux de données :** {qualite_score:.1f}% des données respectent les standards de qualité.")
        st.progress(qualite_score / 100)

        # 4. Détails techniques (Optionnel)
        st.caption(f"✨ **Règles métier appliquées :** Filtrage des prix négatifs, suppression des distances nulles, et validation des IDs zones.")
else:
    selected_month = 6 if mode == "Focus Juin" else 12
    month_name = "Juin" if selected_month == 6 else "Décembre"
    st.title(f"🔍 Diagnostic Opérationnel : {month_name}")

    # SQL pour KPIs + Distribution horaire
    query_kpis = f"SELECT COUNT(*) as n, SUM(total_amount) as r, AVG(fare_amount) as fare FROM fact_trips WHERE trip_month = {selected_month}"
    res_kpi = pd.read_sql(query_kpis, engine).iloc[0]

    f1, f2, f3 = st.columns(3)
    f1.metric("Total Courses", f"{int(res_kpi['n']):,}")
    f2.metric("Chiffre d'Affaires", f"{res_kpi['r']:,.0f} $")
    f3.metric("Course Moyenne (Fare)", f"{res_kpi['fare']:.2f} $")
    st.markdown("---")


    # Requête pour les performances vendeurs
    q_vend = f"""
            SELECT
                v.vendor_name as "Vendeur",
                COUNT(*) as "Nombre de Trajets",
                SUM(f.total_amount) as "Chiffre d'Affaires"
            FROM fact_trips f
            JOIN dim_vendor v ON f.vendor_id = v.vendor_id
            WHERE f.trip_month = {selected_month}
            GROUP BY v.vendor_name
            HAVING COUNT(*) > 0  -- On ne prend que ceux qui ont des données
            ORDER BY COUNT(*) DESC -- Les plus gros en premier
        """
    df_vend = pd.read_sql(q_vend, engine)

    # Affichage sur deux colonnes pour comparer Volume vs CA
    st.subheader(f"🏆 Performance des Vendeurs - {month_name}")
    col_v1, col_v2 = st.columns(2)

    with col_v1:
        fig_vol_v = px.bar(df_vend, x="Nombre de Trajets", y="Vendeur",
                           orientation='h',
                           title="Volume de courses",
                           color="Vendeur",
                           color_discrete_sequence=['#1565C0', '#90CAF9'])
        fig_vol_v.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig_vol_v, width='stretch')

    with col_v2:
        fig_rev_v = px.bar(df_vend, x="Chiffre d'Affaires", y="Vendeur",
                           orientation='h',
                           title="Revenu Total ($)",
                           color="Vendeur",
                           color_discrete_sequence=['#1565C0', '#90CAF9'])
        fig_rev_v.update_layout(showlegend=False, height=300)
        st.plotly_chart(fig_rev_v, width='stretch')

    st.markdown("---")

    # Nouvelle ligne : Analyse par Heure et Zones
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("⏰ Pic d'activité par Heure")
        # On extrait l'heure du timestamp pickup_datetime
        q_hour = f"""
                SELECT EXTRACT(HOUR FROM pickup_datetime) as heure, COUNT(*) as nb
                FROM fact_trips
                WHERE trip_month = {selected_month}
                GROUP BY heure
                ORDER BY heure
            """
        df_hour = pd.read_sql(q_hour, engine)

        # On s'assure que 'heure' est bien traité comme un nombre pour l'axe X
        fig_hour = px.area(df_hour, x='heure', y='nb',
                           labels={'heure': 'Heure de la journée', 'nb': 'Nombre de courses'},
                           color_discrete_sequence=['#FFC107'])

        fig_hour.update_layout(xaxis=dict(tickmode='linear', tick0=0, dtick=3))
        st.plotly_chart(fig_hour, width='stretch')

    with c2:
        st.subheader("📍 Top 10 Destinations")
        q_zones = f"""
            SELECT z.zone, COUNT(*) as volume
            FROM fact_trips f
            JOIN dim_zone z ON f.dropoff_location_id = z.location_id
            WHERE f.trip_month = {selected_month}
            GROUP BY z.zone ORDER BY volume DESC LIMIT 10
        """
        df_zones = pd.read_sql(q_zones, engine)
        fig_zones = px.bar(df_zones, x='volume', y='zone', orientation='h', color='volume', color_continuous_scale='Blues')
        fig_zones.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_zones, width='stretch')
