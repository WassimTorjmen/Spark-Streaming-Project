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


#---------------------------------------------
st.markdown(
    """
    <style>
    .scroll-zone {
        max-height: 500px;          /* hauteur fixe */
        overflow-y: auto;           /* barre de défilement */
        padding-right: 8px;         /* marge pour ne pas masquer le scroll */
    }
    </style>
    """,
    unsafe_allow_html=True
)

#------------------------------
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
        st.error(f"Erreur lecture '{name}' : {e}")
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

def load_top_additives():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM top_additive_products", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'top_additive_products' : {e}")
        return pd.DataFrame()

def load_top_sugary():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM top_sugary_products_by_category", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'top_sugary_products_by_category' : {e}")
        return pd.DataFrame()
    

def load_nova_data():
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        return pd.read_sql("SELECT * FROM nova_group_classification", engine)
    except Exception as e:
        st.error(f"Erreur lecture 'nova_group_classification' : {e}")
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
        st.subheader("Top 9 des catégories principales")
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

    # if not sugar_df.empty:
    #     sugar_df_sorted = sugar_df.sort_values(
    #     by=["main_category", "sugar"],
    #     ascending=[True, False]
    #     ).rename(columns={
    #     "main_category": "Catégorie",
    #     "product_name":  "Nom du produit",
    #     "sugar":         "Sucre (g)"
    #     }).reset_index(drop=True)
    #     with st.expander("Afficher / masquer le Top 10 produits sucrés par catégorie", expanded=False):
    #         st.markdown('<div class="scroll-zone">', unsafe_allow_html=True)
    #
    #         # Boucle catégories triées
    #         for cat in sorted(sugar_df_sorted["Catégorie"].unique()):
    #             with st.expander(cat.title(), expanded=False):
    #                 top10 = (sugar_df_sorted[sugar_df_sorted["Catégorie"] == cat]
    #                          .drop(columns=["batch_id"])
    #                         .head(10)
    #                         .reset_index(drop=True))
    #                 st.dataframe(top10, use_container_width=True)
    #         st.markdown("</div>", unsafe_allow_html=True)
    #
    # else:
    #     st.warning("Aucune donnée à afficher pour `top_sugary_products_by_category`.")


    nova_df = load_nova_data()

    if not nova_df.empty:
        st.subheader("Classification des produits selon les groupes NOVA")

        # ↓↓  moyenne pondérée sur l'entier nova_group
        total = nova_df["product_count"].sum()
        moyenne = (nova_df["nova_group"] * nova_df["product_count"]).sum() / total
        st.markdown(f"Niveau NOVA moyen des produits : **{moyenne:.2f}**")

        # Graphes
        fig = px.bar(
            nova_df.sort_values("nova_group"),
            x="nova_label",
            y="product_count",
            color="nova_group",
            labels={
                "nova_label": "Groupe NOVA (libellé)",
                "product_count": "Nombre de produits",
                "nova_group": "Niveau"
            },
            title="Répartition des produits selon les groupes NOVA"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Aucune donnée dans 'nova_group_classification'.")


# --------------------------------------------
# Page 3 : À propos
elif selected == "À propos":
    st.title("À propos du projet")
    st.write("Cette application Streamlit permet de visualiser les transformations effectuées sur les données Open Food Facts à l'aide de Kafka, Spark et PostgreSQL.")
