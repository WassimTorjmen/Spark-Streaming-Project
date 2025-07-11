import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import sqlalchemy
import plotly.express as px

# --------------------------------------------
# Connexion PostgreSQL
# --------------------------------------------
#est un objet de SQLAlchemy qui contient toutes les infos nécessaires
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

def load_category_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

    try:
        df = pd.read_sql("SELECT * FROM category_counts", engine)
        return df
    except Exception as e:
        st.error(f"Impossible de lire la table 'category_counts' : {e}")
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
                st.success(" Connexion réussie à PostgreSQL")
        except Exception as e:
            st.error(f" Connexion échouée : {e}")
    else:
        st.warning(" Impossible d'établir une connexion.")

# --------------------------------------------
# Page 2 : Transformations depuis le Consumer
# --------------------------------------------
elif selected == "Transformations":
    st.title("Résultat des transformations du Consumer")

    if st.button(" Recharger les données"):
        df = load_nutriscore_data()
        cat_df = load_category_data()
        st.success(" Données rechargées depuis PostgreSQL")
    else:
        df = load_nutriscore_data()
        cat_df = load_category_data()

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
    # Ajout : Affichage des catégories principales (top 8 + Autres)
    # --------------------------------------------
    st.markdown("### Répartition des produits par catégorie Principale")

    if not cat_df.empty:
        top_n = 8
        top = cat_df.nlargest(top_n, "category_count").copy() #Récupère les 8 lignes avec la plus grande valeur de category_count
        other_sum = cat_df["category_count"].sum() - top["category_count"].sum()

        if other_sum > 0: #Si on a bien des "autres", on les ajoute comme 9e catégorie nommée "Autres"
            autres = pd.DataFrame([{
                "main_category": "Autres",
                "category_count": other_sum
            }])
            donut_df = pd.concat([top, autres])
        else:
            donut_df = top

        fig2 = px.pie(
            donut_df,
            names="main_category",
            values="category_count",
            hole=0.4,
            title="Top 8 des catégories principales"
        )
        fig2.update_traces(textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("Aucune donnée à afficher pour `category_counts`.")

