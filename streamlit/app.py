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
        return pd.read_sql("SELECT * FROM nutriscore_counts", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'nutriscore_counts' : {e}")
        return pd.DataFrame()

def load_category_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM category_counts", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'category_counts' : {e}")
        return pd.DataFrame()

def load_brand_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM brand_counts", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'brand_counts' : {e}")
        return pd.DataFrame()

def load_packaging_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM packaging_distribution", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'packaging_distribution' : {e}")
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
# Page 1 : Test PostgreSQL
# --------------------------------------------
if selected == "Test PostgreSQL":
    st.title("Connexion à PostgreSQL")
    engine = get_engine()
    if engine:
        try:
            with engine.connect():
                st.success("Connexion réussie à PostgreSQL")
        except Exception as e:
            st.error(f"Connexion échouée : {e}")
    else:
        st.warning("Impossible d'établir une connexion.")

# --------------------------------------------
# Page 2 : Transformations
# --------------------------------------------
elif selected == "Transformations":
    st.title("Résultat des transformations du Consumer")

    if st.button("🔄 Recharger les données"):
        df = load_nutriscore_data()
        cat_df = load_category_data()
        brand_df = load_brand_data()
        pack_df = load_packaging_data()
        st.success("Données rechargées depuis PostgreSQL")
    else:
        df = load_nutriscore_data()
        cat_df = load_category_data()
        brand_df = load_brand_data()
        pack_df = load_packaging_data()

    if not df.empty:
        st.subheader("Répartition des produits par Nutriscore")
        fig = px.bar(df, x="nutriscore", y="product_count",
                     color="nutriscore",
                     title="Nombre de produits par Nutriscore")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée à afficher pour `nutriscore_counts`.")

    if not cat_df.empty:
        st.subheader("Top 8 des catégories principales")
        top_n = 8
        top = cat_df.nlargest(top_n, "category_count").copy()
        other_sum = cat_df["category_count"].sum() - top["category_count"].sum()
        if other_sum > 0:
            autres = pd.DataFrame([{"main_category": "Autres", "category_count": other_sum}])
            donut_df = pd.concat([top, autres])
        else:
            donut_df = top
        fig2 = px.pie(donut_df, names="main_category", values="category_count", hole=0.4)
        fig2.update_traces(textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("Aucune donnée à afficher pour `category_counts`.")

    if not brand_df.empty:
        st.subheader("Nombre de produits par marque")
        top_brands = brand_df.sort_values("product_count", ascending=False).head(10)
        fig3 = px.bar(top_brands, x="brand", y="product_count",
                      title="Top 10 marques par nombre de produits",
                      labels={"brand": "Marque", "product_count": "Nombre de produits"})
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("Aucune donnée à afficher pour `brand_counts`.")

    if not pack_df.empty:
        st.subheader("Répartition des 10 types d'emballage les plus fréquents")
        top_packaging = pack_df.sort_values("packaging_count", ascending=False).head(10)
        fig4 = px.pie(top_packaging, names="packaging", values="packaging_count", hole=0.3,
                    title="Top 10 types d'emballage")
        fig4.update_traces(textinfo='percent+label')
        st.plotly_chart(fig4, use_container_width=True)

    else:
        st.warning("Aucune donnée à afficher pour `packaging_distribution`.")

# --------------------------------------------
# Page 3 : À propos
# --------------------------------------------
elif selected == "À propos":
    st.title("À propos du projet")
    st.write("Cette application Streamlit permet de visualiser les transformations réalisées à partir des données Open Food Facts, traitées en temps réel via Apache Kafka, Spark Structured Streaming et stockées dans PostgreSQL.")
