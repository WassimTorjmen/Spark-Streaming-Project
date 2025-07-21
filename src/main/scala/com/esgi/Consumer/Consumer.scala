package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import java.util.Properties

object ConsumerKafka {
  private val globalWriteLock = new Object

  val apiResponseSchema = new StructType()
    .add("rows", ArrayType(
      new StructType()
        .add("row",
          new StructType()
            .add("nutriscore_grade", StringType)
            .add("categories_tags", ArrayType(StringType))
            .add("nutriments", ArrayType(
              new StructType()
                .add("name", StringType)
                .add("value", DoubleType)
            ))
            .add("product_name", ArrayType(
              new StructType()
                .add("lang", StringType)
                .add("text", StringType)
            ))
            .add("packaging_tags", ArrayType(StringType))
            .add("brands_tags", ArrayType(StringType))
            .add("additives_tags", ArrayType(StringType))
        )
    ))

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val checkpoint = sys.env.getOrElse("CHECKPOINT_PATH", "checkpoint/generic")

    val spark = SparkSession.builder()
      .appName("ConsumerKafka")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "5")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "openfood")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val parsedStream = rawStream
      .select(from_json(col("value").cast("string"), apiResponseSchema).as("data"))
      .select(explode(col("data.rows")).as("row"))
      .select("row.row.*")

    val transformedStream = parsedStream.transform(applyTransformations)
    val categoryStream = parsedStream.transform(applyCategoryAggregation)
    val sugaryPerCategoryStream = parsedStream.transform(applyTopSugaryProductsByCategory)
    val brandStream = parsedStream.transform(applyBrandAggregation)
    val packagingStream = parsedStream.transform(applyPackagingDistribution)
    
    // 🔹 Additive stream (pas d’agrégation ici)
    val additiveStream = parsedStream.transform(df => {
      val spark = SparkSession.getActiveSession.get
      import spark.implicits._

      df
        .withColumn("product_name_entry", explode($"product_name"))
        .filter($"product_name_entry.lang" === "main")
        .withColumn("product_name", $"product_name_entry.text")
        .withColumn("additive", explode_outer($"additives_tags"))
        .select("product_name", "additive")
    })

    val query = transformedStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Nutriscore Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "nutriscore_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/nutriscore")
      .start()

    val query2 = categoryStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Category Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "category_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/category")
      .start()

    val query3 = sugaryPerCategoryStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Top Sugary Products Per Category Batch $batchId ===")
        val windowSpec = org.apache.spark.sql.expressions.Window
          .partitionBy("main_category")
          .orderBy($"sugar".desc)

        val ranked = batchDF
          .withColumn("rank", row_number().over(windowSpec))
          .filter($"rank" === 1)
          .drop("rank")
          .withColumn("batch_id", lit(batchId))

        ranked.show(1000, truncate = false)
        appendToPostgres(ranked, "top_sugary_products_by_category")
      }
      .outputMode("append")
      .option("checkpointLocation", checkpoint + "/top_sugary_per_category")
      .start()

    val query4 = brandStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Brand Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "brand_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/brand")
      .start()

    val query5 = packagingStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Packaging Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "packaging_distribution")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/packaging")
      .start()

    // 🔹 Additive query avec agrégation dans foreachBatch
    val query6 = additiveStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Top Additive Products Batch $batchId ===")

        val top = batchDF
          .groupBy("product_name")
          .agg(
            count("*").as("additive_count"),
            first("additive").as("most_common_additive")
          )
          .orderBy($"additive_count".desc)
          .limit(10)
          .withColumn("batch_id", lit(batchId))

        top.show(10, truncate = false)
        appendToPostgres(top, "top_additive_products")
      }
      .outputMode("append")
      .option("checkpointLocation", checkpoint + "/top_additive_products")
      .start()

    query.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
    query5.awaitTermination()
    query6.awaitTermination()
  }

  def applyTransformations(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    val transformed = df
      .withColumn("nutriscore",
        when(lower($"nutriscore_grade").isin("a", "b", "c", "d", "e"), upper($"nutriscore_grade"))
          .otherwise("UNKNOWN"))
      .filter($"nutriscore_grade".isNotNull)
      .select("nutriscore")
    transformed
      .groupBy("nutriscore")
      .agg(count("*").as("product_count"))
  }

  def applyCategoryAggregation(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    df
      .withColumn("main_category", $"categories_tags".getItem(0))
      .filter($"main_category".isNotNull)
      .groupBy("main_category")
      .agg(count("*").as("category_count"))
  }

  def applyTopSugaryProductsByCategory(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    val exploded = df
      .withColumn("main_category", lower(trim($"categories_tags".getItem(0))))
      .withColumn("nutriment", explode($"nutriments"))
      .withColumn("product_name_entry", explode($"product_name"))
    exploded
      .filter(
        $"nutriment.name" === "sugars" &&
        $"product_name_entry.lang" === "main" &&
        $"main_category".isNotNull &&
        !$"main_category".isin("en:undefined", "en:null", "undefined", "null", "")
      )
      .withColumn("sugar", $"nutriment.value".cast("double"))
      .withColumn("product_name", $"product_name_entry.text")
      .select("main_category", "product_name", "sugar")
  }

  def applyBrandAggregation(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    df
      .withColumn("brand", $"brands_tags".getItem(0))
      .filter($"brand".isNotNull)
      .groupBy("brand")
      .agg(count("*").as("product_count"))
  }

  def applyPackagingDistribution(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    df
      .withColumn("packaging", $"packaging_tags".getItem(0))
      .filter($"packaging".isNotNull)
      .groupBy("packaging")
      .agg(count("*").as("packaging_count"))
  }

  def writeToPostgres(df: DataFrame, tableName: String): Unit = globalWriteLock.synchronized {
    try {
      val jdbcUrl = sys.env("PG_URL")
      val dbProps = new Properties()
      dbProps.setProperty("user", sys.env("PG_USER"))
      dbProps.setProperty("password", sys.env("PG_PWD"))
      dbProps.setProperty("driver", "org.postgresql.Driver")

      df.write
        .mode("append")
        .jdbc(jdbcUrl, tableName, dbProps)

      println(s"[✓] Write to table: $tableName successful")
    } catch {
      case e: Exception =>
        println(s"[✗] ERROR writing to $tableName: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def appendToPostgres(df: DataFrame, tableName: String): Unit = globalWriteLock.synchronized {
    try {
      val jdbcUrl = sys.env("PG_URL")
      val dbProps = new Properties()
      dbProps.setProperty("user", sys.env("PG_USER"))
      dbProps.setProperty("password", sys.env("PG_PWD"))
      dbProps.setProperty("driver", "org.postgresql.Driver")

      df.write
        .mode("append")
        .jdbc(jdbcUrl, tableName, dbProps)

      println(s"[✓] Append to table: $tableName successful")
    } catch {
      case e: Exception =>
        println(s"[✗] ERROR appending to $tableName: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
