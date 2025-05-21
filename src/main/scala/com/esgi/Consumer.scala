import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ConsumerKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsumerKafkaOpenFood")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "openfood")
      .load()

    val lignes = kafkaDF.selectExpr("CAST(value AS STRING)")

    val schema = new StructType()
      .add("product_name", StringType)
      .add("brands", StringType)
      .add("nutriscore_grade", StringType)
      .add("energy_100g", DoubleType)

    val produits = lignes
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
      .filter(col("product_name").isNotNull)

    val query = produits.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .option("checkpointLocation", "checkpoint/kafka/")
      .start()

    query.awaitTermination()
  }
}
