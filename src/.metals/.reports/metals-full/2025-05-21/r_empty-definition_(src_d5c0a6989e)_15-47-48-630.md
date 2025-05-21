error id: file://<WORKSPACE>/main/scala/com/esgi/Producer.scala:spark.
file://<WORKSPACE>/main/scala/com/esgi/Producer.scala
empty definition using pc, found symbol in pc: spark.
semanticdb not found
empty definition using fallback
non-local guesses:
	 -org/apache/spark/sql/types/org/apache/spark.
	 -ujson/org/apache/spark.
	 -org/apache/spark.
	 -scala/Predef.org.apache.spark.
offset: 92
uri: file://<WORKSPACE>/main/scala/com/esgi/Producer.scala
text:
```scala
package com.esgi

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.sp@@ark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import java.io.PrintWriter
import ujson._

object ProducerSpark {

  val useAPI = true
  val batchLength = 100
  val maxOffset = 200
  val apiURL = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"
  val apiOutputPath = "data/api_data.json"
  val localFilePath = "data/response.json"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkProducerConsumerUnified")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // 1. Préparer les données (API ou fichier)
    val jsonPath = if (useAPI) {
      saveApiToJson(apiOutputPath)
      apiOutputPath
    } else {
      localFilePath
    }

    // 2. Lire en Spark
    val schema = new StructType()
      .add("product_name", StringType)
      .add("brands", StringType)
      .add("nutriscore_grade", StringType)
      .add("energy_100g", DoubleType)

    val df = spark.read
      .schema(schema)
      .json(jsonPath)

    // 3. Transformations Spark
    val produitsFiltres = df
      .filter(col("product_name").isNotNull && col("nutriscore_grade").isNotNull)
      .withColumn("timestamp", current_timestamp())

    // 4. Affichage dans la console
    produitsFiltres.show(truncate = false)
  }

  // ⚠️ Partie Scala classique pour appeler l'API (hors Spark)
  def saveApiToJson(path: String): Unit = {
    var offset = 0
    var allLines = Seq.empty[String]

    while (offset <= maxOffset) {
      val url = s"$apiURL&offset=$offset&length=$batchLength"
      println(s" [API] Requête : $url")
      try {
        val connection = new java.net.URL(url).openConnection().asInstanceOf[java.net.HttpURLConnection]
        connection.setRequestMethod("GET")
        val is = connection.getInputStream
        val content = Source.fromInputStream(is).mkString
        is.close()

        val json = ujson.read(content)
        val products = json("rows").arr.map(_("row").render())
        allLines ++= products
        offset += batchLength
      } catch {
        case e: Exception =>
          println(s"Erreur API : ${e.getMessage}")
          offset = maxOffset + 1
      }
    }

    val out = new PrintWriter(path)
    out.println("[")
    out.println(allLines.mkString(",\n"))
    out.println("]")
    out.close()
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: spark.