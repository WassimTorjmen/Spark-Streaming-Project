-- Table 1 : Répartition par Nutriscore
CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) PRIMARY KEY,
  product_count INTEGER
);

-- Table 2 : Répartition par catégorie principale
CREATE TABLE IF NOT EXISTS category_counts (
  main_category VARCHAR(255) PRIMARY KEY,
  category_count INTEGER
);


-- Table 1 : Répartition par Nutriscore
CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) PRIMARY KEY,
  product_count INTEGER
);

-- Table 2 : Répartition par catégorie principale
CREATE TABLE IF NOT EXISTS category_counts (
  main_category VARCHAR(255) PRIMARY KEY,
  category_count INTEGER
);

-- Table 3 : Nombre de produits par marque
CREATE TABLE IF NOT EXISTS brand_counts (
  brand VARCHAR(255) PRIMARY KEY,
  product_count INTEGER
);

-- Table 4 : Répartition par type d’emballage
CREATE TABLE IF NOT EXISTS packaging_distribution (
  packaging VARCHAR(255) PRIMARY KEY,
  packaging_count INTEGER
);
