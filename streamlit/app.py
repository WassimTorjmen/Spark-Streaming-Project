import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import sqlalchemy
import plotly.express as px

# --------------------------------------------
# Connexion PostgreSQL
# --------------------------------------------
def get_engine():
    try:
        engine = sqlalchemy.create_engine(
            "postgresql://ingest:ingestpwd@postgres:5432/openfood"
        )
        return engine
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return None

# --------------------------------------------
# Charger les résultats des transformations
# --------------------------------------------
def load_nutriscore_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql("SELECT * FROM nutriscore_counts", engine)
        return df
    except Exception as e:
        st.error(f"Impossible de lire la table 'nutriscore_counts' : {e}")
        return pd.DataFrame()

# --------------------------------------------
# Menu principal
# --------------------------------------------
with st.sidebar:
    selected = option_menu(
        menu_title="Dashboard",
        options=["Test PostgreSQL", "Transformations", "À propos"],
        icons=["database", "bar-chart", "info-circle"],
        default_index=0,
    )

# --------------------------------------------
# Page 1 : Test de connexion
# --------------------------------------------
if selected == "Test PostgreSQL":
    st.title("Connexion à PostgreSQL")
    engine = get_engine()
    if engine is not None:
        try:
            with engine.connect():
                st.success("✅ Connexion réussie à PostgreSQL")
        except Exception as e:
            st.error(f"❌ Connexion échouée : {e}")
    else:
        st.warning("⚠️ Impossible d'établir une connexion.")

# --------------------------------------------
# Page 2 : Transformations depuis le Consumer
# --------------------------------------------
elif selected == "Transformations":
    st.title("Résultat des transformations du Consumer")

    if st.button("🔄 Recharger les données"):
        df = load_nutriscore_data()
        st.success("✅ Données rechargées depuis PostgreSQL")
    else:
        df = load_nutriscore_data()

    if not df.empty:
        st.write(f"{len(df)} lignes chargées depuis PostgreSQL.")
        st.dataframe(df)

        st.markdown("### Répartition des produits par Nutriscore")
        fig = px.bar(df, x="nutriscore", y="product_count",
                     labels={"nutriscore": "Nutriscore", "product_count": "Nombre de produits"},
                     color="nutriscore",
                     title="Nombre de produits par Nutriscore")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée à afficher pour `nutriscore_counts`.")

# --------------------------------------------
# Page 3 : À propos
# --------------------------------------------
elif selected == "À propos":
    st.title("📘 À propos")
    st.markdown("""
    - **Projet** : Pipeline Kafka → Spark → PostgreSQL → Streamlit  
    - **Visualisation** : Résultats affichés dynamiquement  
    - **Auteur** : Ton Nom ✨
    """)
