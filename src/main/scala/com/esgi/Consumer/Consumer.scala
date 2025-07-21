package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import java.util.Properties

object ConsumerKafka {
  // Define schema for the complete API response
  val apiResponseSchema = new StructType()
    .add("rows", ArrayType(
      new StructType()
        .add("row",
          new StructType()
            .add("nutriscore_grade", StringType)
            .add("categories_tags", ArrayType(StringType))
            .add("packaging_tags", ArrayType(StringType))
            .add("brands_tags", ArrayType(StringType))
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

    // Read from Kafka
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "openfood")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    // Parse and extract JSON structure
    val parsedStream = rawStream
      .select(from_json(col("value").cast("string"), apiResponseSchema).as("data"))
      .select(explode(col("data.rows")).as("row"))
      .select("row.row.*")

    // Apply transformations
    val transformedStream = parsedStream.transform(applyTransformations)
    val categoryStream = parsedStream.transform(applyCategoryAggregation)
    val brandStream = parsedStream.transform(applyBrandAggregation)
    val packagingStream = parsedStream.transform(applyPackagingDistribution)

    // Nutriscore count
    val query = transformedStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Nutriscore Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "nutriscore_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/nutriscore")
      .start()

    // Category count
    val query2 = categoryStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Category Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "category_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/category")
      .start()

    // Brand count
    val query3 = brandStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Brand Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "brand_counts")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/brand")
      .start()

    // Packaging distribution
    val query4 = packagingStream.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        println(s"=== Packaging Batch $batchId ===")
        batchDF.show(1000, truncate = false)
        writeToPostgres(batchDF, "packaging_distribution")
      }
      .outputMode("complete")
      .option("checkpointLocation", checkpoint + "/packaging")
      .start()

    // Await termination of all queries
    query.awaitTermination()
    query2.awaitTermination()
    query3.awaitTermination()
    query4.awaitTermination()
  }

  // Nutriscore Transformation
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

  // Category Aggregation
  def applyCategoryAggregation(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    df
      .withColumn("main_category", $"categories_tags".getItem(0))
      .filter($"main_category".isNotNull)
      .groupBy("main_category")
      .agg(count("*").as("category_count"))
  }

  // Brand Aggregation (NEW)
  def applyBrandAggregation(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    df
      .withColumn("brand", $"brands_tags".getItem(0))
      .filter($"brand".isNotNull)
      .groupBy("brand")
      .agg(count("*").as("product_count"))
  }

  // Packaging Aggregation (NEW)
  def applyPackagingDistribution(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._

    df
      .withColumn("packaging", $"packaging_tags".getItem(0))
      .filter($"packaging".isNotNull)
      .groupBy("packaging")
      .agg(count("*").as("packaging_count"))
  }

  // PostgreSQL writer
  def writeToPostgres(df: DataFrame, tableName: String): Unit = {
    val jdbcUrl = sys.env("PG_URL")
    val dbProps = new Properties()
    dbProps.setProperty("user", sys.env("PG_USER"))
    dbProps.setProperty("password", sys.env("PG_PWD"))
    dbProps.setProperty("driver", "org.postgresql.Driver")

    df.write
      .mode("overwrite")
      .jdbc(jdbcUrl, tableName, dbProps)
  }
}
