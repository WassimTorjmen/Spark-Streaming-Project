CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) PRIMARY KEY,
  product_count INTEGER
);

CREATE TABLE IF NOT EXISTS category_counts (
  main_category VARCHAR(255) PRIMARY KEY,
  category_count INTEGER
);

CREATE TABLE IF NOT EXISTS top_sugary_products_by_category (
    main_category TEXT,
    product_name TEXT,
    sugar DOUBLE PRECISION,
    batch_id BIGINT
);

