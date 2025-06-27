package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener, Trigger}
import org.apache.spark.sql.types._
import scala.util.{Try, Success, Failure}
import scala.io.Source
import java.util.concurrent.TimeUnit

object FetchAndTransform {
  // Configuration API
  val API_BASE_URL = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"
  val BATCH_SIZE = 10
  val MAX_RETRIES = 5
  val RETRY_DELAY_MS = 30000 // 30 seconds
  val REQUEST_RATE = 2 // requests per second
  val MAX_CONSECUTIVE_FAILURES = 5

  // Configuration PostgreSQL
  val DB_URL = "jdbc:postgresql://localhost:5433/food_analysis"
  val DB_USER = "postgres"
  val DB_PASSWORD = "postgres"
  val DB_CONNECTION_POOL = "numPartitions=5;maxPoolSize=10;minPoolSize=5"

  // Define schema for JSON data
  val productSchema = new StructType()
    .add("rows", ArrayType(
      new StructType()
        .add("row", new StructType()
          .add("nutriscore_grade", StringType)
          .add("categories_tags", ArrayType(StringType))
        )
    ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FoodAnalysisStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "5")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint/food_analysis")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    // Variables for streaming state
    var currentOffset = 0
    var consecutiveFailures = 0

    // Add complete query listener
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"🚀 Query started: ${event.id}")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println(s"📊 Query progress: ${event.progress}")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        event.exception.foreach { ex =>
          println(s"🚨 Query terminated with exception: $ex")
        }
        println("ℹ️ Shutting down SparkSession...")
        spark.close()
      }
    })

    // Create a streaming DataFrame that will track our current offset
    val offsetStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", REQUEST_RATE)
      .load()
      .select(lit(currentOffset).as("current_offset"))

    // Define the streaming query with Complete output mode
    val query = offsetStream
      .groupBy()
      .agg(max("current_offset").as("max_offset"))
      .writeStream
      .outputMode("complete")  // Changed from append to complete
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
        println(s"\n=== Processing batch $batchId ===")
        processBatch(currentOffset, batchId, spark) match {
          case Some(newOffset) =>
            currentOffset = newOffset
            consecutiveFailures = 0
          case None =>
            consecutiveFailures += 1
            if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
              println(s"⚠️ Too many consecutive failures ($consecutiveFailures), stopping...")
              spark.streams.active.foreach(_.stop())
            }
        }
      }
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "checkpoint/food_analysis")
      .start()

    // Graceful shutdown hook
    sys.addShutdownHook {
      println("\n🛑 Shutdown signal received. Stopping gracefully...")
      Try(query.stop()).recover {
        case e => println(s"⚠️ Error stopping query: ${e.getMessage}")
      }
      Try(spark.close()).recover {
        case e => println(s"⚠️ Error closing SparkSession: ${e.getMessage}")
      }
      println("👋 Application shutdown complete")
    }

    query.awaitTermination()
  }

  def fetchData(offset: Int, retryCount: Int = 0)(implicit spark: SparkSession): Option[DataFrame] = {
    import spark.implicits._

    val url = s"$API_BASE_URL&offset=$offset&length=$BATCH_SIZE"
    println(s"📦 Fetching from offset: $offset (attempt ${retryCount + 1})")

    Try {
      val jsonStr = Source.fromURL(url).mkString
      if (jsonStr.isEmpty) {
        println("⚠️ Received empty response")
        None
      } else {
        val productsDF = spark.read
          .schema(productSchema)
          .option("multiLine", true)
          .json(Seq(jsonStr).toDS())
          .select(explode($"rows").as("row"))
          .select(
            $"row.row.nutriscore_grade".as("nutriscore_grade"),
            $"row.row.categories_tags".as("categories_tags"),
          )
        productsDF.show(truncate = false)  

        val count = productsDF.count()
        if (count > 0) {
          println(s"✅ Successfully fetched $count products")
          Some(productsDF)
        } else {
          println("⚠️ No products found in response")
          None
        }
      }
    } match {
      case Success(result) => result
      case Failure(e) if e.getMessage.contains("429") && retryCount < MAX_RETRIES =>
        println(s"⚠️ Rate limited, retrying in ${RETRY_DELAY_MS/1000} seconds...")
        TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MS)
        fetchData(offset, retryCount + 1)
      case Failure(e) =>
        println(s"❌ Failed to fetch data: ${e.getMessage}")
        None
    }
  }

  def processBatch(offset: Int, batchId: Long, spark: SparkSession): Option[Int] = {
    implicit val ss: SparkSession = spark
    import spark.implicits._

    fetchData(offset).flatMap { productsDF =>
      if (productsDF.count() == 0) {
        println("ℹ️ No data to process in this batch")
        None
      } else {
        // 1. Nutriscore analysis - using complete output mode
        val nutriscoreDF = productsDF
          .withColumn("nutriscore", 
            when(lower($"nutriscore_grade").isin("a", "b", "c", "d", "e"), upper($"nutriscore_grade"))
            .otherwise("UNKNOWN"))
          .filter($"nutriscore_grade".isNotNull)
          .groupBy("nutriscore")
          .agg(
            count("*").as("product_count"),
            max(lit(batchId)).as("batch_id"),
            max(current_timestamp()).as("timestamp")
          )

        // 2. Categories analysis - using complete output mode
        val categoriesDF = productsDF
          .withColumn("category", explode($"categories_tags"))
          .filter($"category".isNotNull && !$"category".isin("en:null", "null"))
          .withColumn("category", regexp_replace($"category", "en:", ""))
          .groupBy("category")
          .agg(
            count("*").as("product_count"),
            max(lit(batchId)).as("batch_id"),
            max(current_timestamp()).as("timestamp")
          )

        // Save to PostgreSQL - using Overwrite mode since we're using Complete output
        saveToPostgres(nutriscoreDF, "nutriscore_counts_current", SaveMode.Overwrite)
        saveToPostgres(categoriesDF, "categories_counts_current", SaveMode.Overwrite)

        println(s"✅ Successfully processed batch $batchId")
        Some(offset + BATCH_SIZE)
      }
    }
  }

  def saveToPostgres(df: DataFrame, tableName: String, mode: SaveMode): Unit = {
    Try {
      // Show the DataFrame content in console first
      println(s"\n📊 Showing DataFrame content for table: $tableName")
      df.show(numRows = 20, truncate = false)
      
      df.write
        .format("jdbc")
        .option("url", DB_URL)
        .option("dbtable", tableName)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("connectionProperties", DB_CONNECTION_POOL)
        .mode(mode)
        .save()
      println(s"💾 Saved ${df.count()} rows to $tableName")
    }.recover {
      case e: Exception =>
        println(s"❌ Failed to save to $tableName: ${e.getMessage}")
    }
  }
}