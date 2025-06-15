package com.esgi

import java.sql.{Connection, DriverManager}

object DatabaseInitializer {
  val DB_URL = "jdbc:postgresql://localhost:5433/food_analysis"
  val DB_USER = "postgres"
  val DB_PASSWORD = "postgres"

  def main(args: Array[String]): Unit = {
    val conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)
    try {
      createTables(conn)
      println("Les tables ont été créées avec succès.")
    } finally {
      conn.close()
    }
  }

  def createTables(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    
    stmt.execute("""
      CREATE TABLE IF NOT EXISTS products (
        code VARCHAR(50) PRIMARY KEY,
        product_name TEXT,
        brands TEXT,
        nutriscore_grade VARCHAR(50),
        nutriscore_score INTEGER,
        energy_kcal DOUBLE PRECISION,
        fat DOUBLE PRECISION,
        saturated_fat DOUBLE PRECISION,
        carbohydrates DOUBLE PRECISION,
        sugars DOUBLE PRECISION,
        proteins DOUBLE PRECISION,
        salt DOUBLE PRECISION,
        cocoa_percentage DOUBLE PRECISION,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS allergens (
        id SERIAL PRIMARY KEY,
        product_code VARCHAR(50) REFERENCES products(code) ON DELETE CASCADE,
        allergen TEXT,
        UNIQUE(product_code, allergen)
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS labels (
        id SERIAL PRIMARY KEY,
        product_code VARCHAR(50) REFERENCES products(code) ON DELETE CASCADE,
        label TEXT,
        UNIQUE(product_code, label)
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS categories (
        id SERIAL PRIMARY KEY,
        product_code VARCHAR(50) REFERENCES products(code) ON DELETE CASCADE,
        category TEXT,
        UNIQUE(product_code, category)
      )
    """)

    stmt.execute("""
      CREATE TABLE IF NOT EXISTS packagings (
        id SERIAL PRIMARY KEY,
        product_code VARCHAR(50) REFERENCES products(code) ON DELETE CASCADE,
        material TEXT,
        shape TEXT,
        recycling TEXT,
        quantity TEXT
      )
    """)

    stmt.close()
  }
}
