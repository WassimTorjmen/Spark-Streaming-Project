-- Table 1 : Répartition par Nutriscore
CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) ,
  product_count INTEGER
);

-- Table 2 : Répartition par catégorie principale
CREATE TABLE IF NOT EXISTS category_counts (
  main_category VARCHAR(255) ,
  category_count INTEGER
);


-- Table 1 : Répartition par Nutriscore
CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) ,
  product_count INTEGER
);

-- Table 2 : Répartition par catégorie principale
CREATE TABLE IF NOT EXISTS category_counts (
  main_category VARCHAR(255) ,
  category_count INTEGER
);

-- Table 3 : Nombre de produits par marque
CREATE TABLE IF NOT EXISTS brand_counts (
  brand VARCHAR(255) ,
  product_count INTEGER
);

-- Table 4 : Répartition par type d’emballage
CREATE TABLE IF NOT EXISTS packaging_distribution (
  packaging VARCHAR(255) ,
  packaging_count INTEGER
);

-- Table 5 : Produits avec le plus d’additifs
CREATE TABLE IF NOT EXISTS top_additive_products (
  product_name VARCHAR(255),
  additive_count INTEGER,
  most_common_additive VARCHAR(255),
  batch_id INTEGER
);
-- Table 6 : Produits avec le plus de sucre par catégorie

CREATE TABLE IF NOT EXISTS top_sugary_products_by_category (
  main_category VARCHAR(255),
  product_name VARCHAR(255),
  sugar DOUBLE PRECISION,
  batch_id INTEGER
);
CREATE TABLE IF NOT EXISTS nova_group_classification (
  nova_group      INT,
  nova_label      VARCHAR(255),
  product_count   INT
);