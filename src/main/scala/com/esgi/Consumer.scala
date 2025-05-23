package com.esgi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ConsumerKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsumerKafka")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val lignes = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "openfood")
      .option("startingOffsets", "earliest")   // ← important
      .load()
      .selectExpr("CAST(value AS STRING) AS message")

    val query = lignes.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", false)
      .option("numRows", 5)                    // n’affiche que 5 messages par batch
      .option("checkpointLocation", "checkpoint/generic")
      .start()

    query.awaitTermination()
  }
}