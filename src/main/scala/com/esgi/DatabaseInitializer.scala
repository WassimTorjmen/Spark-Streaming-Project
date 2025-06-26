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

  // Drop tables if they exist (for development only - remove in production)
  stmt.execute("DROP TABLE IF EXISTS nutriscore_counts_current")
  stmt.execute("DROP TABLE IF EXISTS nutriment_aggregates")
  stmt.execute("DROP TABLE IF EXISTS categories_counts_current")

  // Create tables with improved schema
  stmt.execute("""
    CREATE TABLE nutriscore_counts_current (
      batch_id BIGINT,
      timestamp TIMESTAMP,
      nutriscore VARCHAR(10),
      product_count INTEGER DEFAULT 0
    )
  """)

  stmt.execute("""
    CREATE TABLE categories_counts_current (
      batch_id BIGINT,
      timestamp TIMESTAMP,
      category VARCHAR(255),
      product_count INTEGER DEFAULT 0
    )
  """)

  // Add indexes for better query performance
  stmt.execute("CREATE INDEX idx_nutriscore_batch ON nutriscore_counts_current(batch_id)")
  stmt.execute("CREATE INDEX idx_nutriscore_grade ON nutriscore_counts_current(nutriscore)")
  stmt.execute("CREATE INDEX idx_category_batch ON categories_counts_current(batch_id)")
  stmt.execute("CREATE INDEX idx_category_name ON categories_counts_current(category)")

  stmt.close()
}
}
