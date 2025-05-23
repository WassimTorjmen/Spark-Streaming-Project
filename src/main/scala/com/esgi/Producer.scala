package com.esgi

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.net.{URL, HttpURLConnection}
import scala.io.Source
import ujson._

object ProducerKafka {

  val baseUrl =
    "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"

  def main(args: Array[String]): Unit = {

    /* --------- Paramètres --------- */
    val useAPI      = true          // false ⇒ lit food.parquet
    val jsonPath    = "data/food.parquet" // fichier de teste 
    val batchLength = 100
    val maxOffset   = 3808300          // pour tester après on fait 3808300 ensuite
    val topic       = "openfood"

    /*Config Kafka */
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer",
              "org.apache.kafka.common.serialization.StringSerializer")
              props.put("value.serializer",
              "org.apache.kafka.common.serialization.StringSerializer")
   props.put("max.request.size", "2000000")

    val producer = new KafkaProducer[String, String](props)
    println("Producer Kafka – démarrage")

  
    if (useAPI) {
      var offset = 0
      while (offset <= maxOffset) {
        val url   = s"$baseUrl&offset=$offset&length=$batchLength"
        val batch = fetchBatchFromAPI(url)          // JSON brut

        if (batch.nonEmpty) {
          producer.send(new ProducerRecord(topic, null, batch))
          println(s"Batch offset=$offset envoyé (${batch.length} chars)")
          // preview des données
          val preview = if (batch.length > 200) batch.take(200) + "..." else batch
          println(s"🟢 Batch offset=$offset envoyé  (${batch.length} chars)")
          println(s"   ↳ Aperçu : $preview\n")
          //reduire le temps d'attente
          Thread.sleep(2000)
        } else {
          println(s"API vide à offset $offset, arrêt.")
        }
        offset += batchLength
        Thread.sleep(2000)
      }
    } else {
      fetchBatchesFromFile(jsonPath).foreach { batch =>
        producer.send(new ProducerRecord(topic, null, batch))
        println(s"Batch fichier envoyé (${batch.length} chars)")
        Thread.sleep(1000)
      }
    }

    producer.flush()
    producer.close()
    println(" Fin d’envoi – producer Kafka fermé.")
  }

  /* Helpers*/

  /** Télécharge un bloc JSON (100 lignes) et renvoie la chaîne brute. */
  def fetchBatchFromAPI(url: String): String = {
    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(5000)
      conn.setReadTimeout(5000)

      val is   = conn.getInputStream
      val json = Source.fromInputStream(is).mkString
      is.close()
      json
    } catch {
      case e: Exception =>
        println(s" API error : ${e.getMessage}")
        ""
    }
  }

  /** Découpe un fichier local en blocs JSON de 100 produits. */
  def fetchBatchesFromFile(path: String): Vector[String] = {
    try {
      val raw   = Source.fromFile(path).mkString
      val root  = ujson.read(raw)
      // à enlever 
      val rows  = root("rows").arr.map(_("row"))
      rows.grouped(100).map(g => ujson.Arr(g: _*).render()).toVector
    } catch {
      case e: Exception =>
        println(s" File error : ${e.getMessage}")
        Vector.empty
    }
  }
}
