package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._


object ConsumerKafka {
  // Define schema for product data (similar to your FetchAndTransform)
  val productSchema = new StructType()
    .add("nutriscore_grade", StringType)
    .add("categories_tags", ArrayType(StringType))

  def main(args: Array[String]): Unit = {
    // Get environment variables
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

    // Parse JSON and apply transformations
    val transformedStream = rawStream
      .select(from_json(col("value").cast("string"), productSchema).as("data"))
      .select("data.*")
      .transform(applyTransformations) // Apply your transformations

    // Write to console (or other sinks)
    val query = transformedStream.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", "20") // Increased to show more data
      .option("checkpointLocation", checkpoint)
      .start()

    query.awaitTermination()
  }

 def applyTransformations(df: DataFrame): DataFrame = {
    val spark = SparkSession.getActiveSession.get
    import spark.implicits._
    
    // 1. Nutriscore analysis
    val nutriscoreDF = df
      .withColumn("nutriscore", 
        when(lower($"nutriscore_grade").isin("a", "b", "c", "d", "e"), upper($"nutriscore_grade"))
        .otherwise("UNKNOWN"))
      .filter($"nutriscore_grade".isNotNull)
      .groupBy("nutriscore")
      .agg(count("*").as("product_count"))

    // 2. Categories analysis
    val categoriesDF = df
      .withColumn("category", explode($"categories_tags"))
      .filter($"category".isNotNull && !$"category".isin("en:null", "null"))
      .withColumn("category", regexp_replace($"category", "en:", ""))
      .groupBy("category")
      .agg(count("*").as("product_count"))

    // Join the results if needed, or return one of them
    // Here we'll return the nutriscore analysis as the main output
    nutriscoreDF
  }
}