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
    //Paramètres 
    val batchLength = 100
    val maxOffset   = 3808300
    val topic       = "openfood"
    val bootstrap   =
      sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    //Configuration Kafka 
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("max.request.size", "2000000")

    val producer = new KafkaProducer[String, String](props)
    println("Producer Kafka – démarrage")

    // === ta boucle d’origine, inchangée ===
    var offset = 0
    while (offset <= maxOffset) {
      val url   = s"$baseUrl&offset=$offset&length=$batchLength"
      val batch = fetchBatchFromAPI(url)

      if (batch.nonEmpty) {
        producer.send(new ProducerRecord(topic, null, batch))
        println(s" Batch offset=$offset envoyé (${batch.length} chars)")

        val preview = if (batch.length > 200) batch.take(200) + "..." else batch
        println(s"   ↳ Aperçu : $preview\n")
        Thread.sleep(2000)
      } else {
        println(s"Pas de données à offset $offset, je continue…")
      }

      offset += batchLength
      Thread.sleep(2000)
    }

    producer.flush()
    producer.close()
    println("Fin d’envoi – producer Kafka fermé.")
  }

  
  def fetchBatchFromAPI(url: String): String = {
    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)
      val is   = conn.getInputStream
      val json = Source.fromInputStream(is).mkString
      is.close()
      json
    } catch {
      case e: Exception =>
        println(s"API error : ${e.getMessage}")
        ""
    }
  }
}
