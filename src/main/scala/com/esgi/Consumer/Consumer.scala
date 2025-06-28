package com.esgi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ConsumerKafka {
  def main(args: Array[String]): Unit = {

    // récupère l'env injecté par Docker
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val checkpoint = sys.env.getOrElse("CHECKPOINT_PATH", "checkpoint/generic")

    val spark = SparkSession.builder()
      .appName("ConsumerKafka")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val lignes = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", "openfood")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) AS message")

    val query = lignes.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("numRows", "5")
      .option("checkpointLocation", checkpoint)
      .start()

    query.awaitTermination()
  }
}
