package com.esgi
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Consumer {

  def main(args: Array[String]): Unit = {
    // Créer la session Spark
    val spark = SparkSession.builder()
      .appName("OpenFoodFactsConsumer")
      .master("local[*]") // local multi-thread
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Lire depuis le socket localhost:9999
    val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Traitement : ici on affiche juste les lignes reçues
    val lines = df.withColumn("timestamp", current_timestamp())

    val query = lines.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .option("checkpointLocation", "D:/ESGI/Spark Streaming/spark-streaming-project/checkpoint")
      .start()

    query.awaitTermination()
  }
}