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

    // Nettoyage / filtrage des lignes vides ou trop courtes
    val filtered = df.filter(length(col("value")) > 10)

    // Affichage brut ligne par ligne
    val query = filtered.writeStream
      .outputMode("append")
      .foreachBatch { (batchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        batchDF.write
          .format("console")
          .option("truncate", "false")
          .save()
      }
      .option("checkpointLocation", "C:/Users/Abdellatif/Desktop/Spark-Streaming-Project/checkpoint")
      .start()

    query.awaitTermination()
  }
}
