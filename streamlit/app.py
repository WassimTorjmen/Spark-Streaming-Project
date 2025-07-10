import streamlit as st
from streamlit_option_menu import option_menu
import sqlalchemy

# --------------------------------------------
# Connexion PostgreSQL (juste pour tester)
# --------------------------------------------
def test_postgres_connection():
    try:
        engine = sqlalchemy.create_engine(
            "postgresql://ingest:ingestpwd@postgres:5432/openfood"
        )
        # Test passif : ouverture de connexion uniquement, pas de requête
        with engine.connect():
            pass
        return True
    except Exception as e:
        st.error(f"❌ Connexion échouée à PostgreSQL : {e}")
        return False

# --------------------------------------------
# Menu
# --------------------------------------------
with st.sidebar:
    selected = option_menu(
        menu_title="Dashboard",
        options=["Test PostgreSQL", "À propos"],
        icons=["database", "info-circle"],
        default_index=0,
    )

# --------------------------------------------
# Pages
# --------------------------------------------
if selected == "Test PostgreSQL":
    st.title("🔌 Connexion à PostgreSQL")

    if test_postgres_connection():
        st.success("✅ Connexion réussie à PostgreSQL")
    else:
        st.warning("⚠️ Impossible de se connecter à PostgreSQL.")

elif selected == "À propos":
    st.title("À propos")
    st.markdown("""
    - Projet : Pipeline Kafka / Spark / PostgreSQL  
    - Visualisation : Streamlit  
    - Auteur : Ton Nom
    """)
