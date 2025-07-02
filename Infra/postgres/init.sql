CREATE TABLE IF NOT EXISTS nutriscore_counts (
  nutriscore VARCHAR(10) NOT NULL,
  product_count BIGINT NOT NULL,
  PRIMARY KEY (nutriscore)
);

CREATE TABLE IF NOT EXISTS categories_counts (
  category VARCHAR(255) NOT NULL,
  product_count BIGINT NOT NULL,
  PRIMARY KEY (category)
);
