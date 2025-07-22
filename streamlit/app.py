import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import sqlalchemy
import plotly.express as px

# Connexion PostgreSQL
def get_engine():
    try:
        return sqlalchemy.create_engine(
            "postgresql://ingest:ingestpwd@postgres:5432/openfood"
        )
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        return None

# Lecture d'une table PostgreSQL
def load_table(name):
    try:
        engine = get_engine()
        return pd.read_sql(f"SELECT * FROM {name}", engine)
    except Exception as e:
        st.error(f"Erreur lecture '{name}' : {e}")
        return pd.DataFrame()

# Menu latéral
with st.sidebar:
    selected = option_menu(
        menu_title="Dashboard",
        options=["Test PostgreSQL", "Transformations", "À propos"],
        icons=["database", "bar-chart", "info-circle"],
        default_index=0,
    )

# Page 1 : Test de connexion
if selected == "Test PostgreSQL":
    st.title("Connexion à PostgreSQL")
    try:
        with get_engine().connect():
            st.success("Connexion réussie à PostgreSQL")
    except Exception as e:
        st.error(f"Connexion échouée : {e}")

# Page 2 : Transformations
elif selected == "Transformations":
    st.title("Résultat des transformations")

    if st.button("🔄 Recharger les données"):
        st.session_state["df"] = load_table("nutriscore_counts")
        st.session_state["cat_df"] = load_table("category_counts")
        st.session_state["brand_df"] = load_table("brand_counts")
        st.session_state["pack_df"] = load_table("packaging_distribution")
        st.session_state["add_df"] = load_table("top_additive_products")
        st.session_state["sugar_df"] = load_table("top_sugary_products_by_category")

    df = st.session_state.get("df", pd.DataFrame())
    cat_df = st.session_state.get("cat_df", pd.DataFrame())
    brand_df = st.session_state.get("brand_df", pd.DataFrame())
    pack_df = st.session_state.get("pack_df", pd.DataFrame())
    add_df = st.session_state.get("add_df", pd.DataFrame())
    sugar_df = st.session_state.get("sugar_df", pd.DataFrame())

    if not df.empty:
        st.subheader("Répartition des produits par Nutriscore")
        fig = px.bar(df, x="nutriscore", y="product_count", color="nutriscore")
        st.plotly_chart(fig, use_container_width=True)

    if not cat_df.empty:
        st.subheader("Top 8 des catégories principales")
        top = cat_df.nlargest(8, "category_count").copy()
        autres = pd.DataFrame([{
            "main_category": "Autres",
            "category_count": cat_df["category_count"].sum() - top["category_count"].sum()
        }])
        donut_df = pd.concat([top, autres])
        fig2 = px.pie(donut_df, names="main_category", values="category_count", hole=0.4)
        fig2.update_traces(textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)

    if not brand_df.empty:
        st.subheader("Top 10 marques par nombre de produits")
        top_brands = brand_df.sort_values("product_count", ascending=False).head(10)
        fig3 = px.bar(top_brands, x="brand", y="product_count")
        st.plotly_chart(fig3, use_container_width=True)

    if not pack_df.empty:
        st.subheader("Top 10 types d'emballage")
        top_pack = pack_df.sort_values("packaging_count", ascending=False).head(10)
        fig4 = px.pie(top_pack, names="packaging", values="packaging_count", hole=0.3)
        fig4.update_traces(textinfo='percent+label')
        st.plotly_chart(fig4, use_container_width=True)

    if not add_df.empty:
        st.subheader("Top 10 produits avec le plus d'additifs")
        st.dataframe(add_df[["product_name", "additive_count", "most_common_additive"]].head(10), use_container_width=True)

    if not sugar_df.empty:
        st.subheader("Produit le plus sucré par catégorie")
        sugar_df_sorted = sugar_df.sort_values(by=["main_category", "sugar"], ascending=[True, False])
        sugar_df_sorted = sugar_df_sorted.rename(columns={
            "main_category": "Catégorie",
            "product_name": "Nom du produit",
            "sugar": "Sucre (g)"
        })
        st.dataframe(sugar_df_sorted, use_container_width=True)

# Page 3 : À propos
elif selected == "À propos":
    st.title("À propos du projet")
    st.write("Cette application Streamlit permet de visualiser les transformations effectuées sur les données Open Food Facts à l'aide de Kafka, Spark et PostgreSQL.")
