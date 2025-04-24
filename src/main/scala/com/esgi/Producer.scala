package com.esgi
import ujson._

import java.io._
import java.net._
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Producer {

  val apiBaseUrl =
    "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"

  def main(args: Array[String]): Unit = {
    println("[Producer] Démarrage...")

    val port = 9999
    val jsonPath = "data/response.json"
    val useAPI = true // ← change à true pour utiliser l’API

    try {
      val serverSocket = new ServerSocket(port)
      println(s"[Producer] Socket lancé sur le port $port. En attente de client...")

      val socket = serverSocket.accept()
      println("[Producer] Client connecté. Début de l’envoi...")

      val out = new PrintWriter(socket.getOutputStream, true)

      if (useAPI) {
        println("[Producer] Mode API activé")
        streamFromAPI(out)
      } else {
        println("[Producer] Lecture locale du fichier JSON")
        streamFromJsonFile(out, jsonPath)
      }

      println("[Producer] Envoi terminé. Le Producer reste actif.")
      while (true) Thread.sleep(1000)

    } catch {
      case e: Exception =>
        println(s"[Producer] Exception : ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def streamFromAPI(out: PrintWriter): Unit = {
    var offset = 0
    val length = 100
    val maxOffset = 3808300

    try {
      while (offset <= maxOffset) {
        val url = s"$apiBaseUrl&offset=$offset&length=$length"
        println(s"[API] Requête : $url")

        val connection = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.setConnectTimeout(5000)
        connection.setReadTimeout(5000)

        val is = connection.getInputStream
        val content = Source.fromInputStream(is).mkString
        is.close()

        val json = ujson.read(content)

      // Extraire les produits
        val rows = json("rows").arr
        println(s"[API] ${rows.length} produits trouvés à offset $offset")

        rows.foreach { r =>
          val productJson = r("row") // <- l'objet produit
          val line = productJson.toString()
          out.println(line)
          println(s"[SEND API] $line")
          Thread.sleep(500)
        }

      offset += length
      }
    } catch {
      case e: Exception =>
        println(s"[API] Erreur : ${e.getMessage}")
    }
  }

  def streamFromJsonFile(out: PrintWriter, path: String): Unit = {
    try {
      val spark = SparkSession.builder()
        .appName("JsonFileProducer")
        .master("local[*]")
        .getOrCreate()

      val df = spark.read.json(path)

      df.collect().foreach { row =>
        out.println(row.toString())
        println(s"[SEND FILE] ${row.toString()}")
        Thread.sleep(500)
      }

      spark.stop()
    } catch {
      case e: Exception =>
        println(s"[FILE] Erreur lors de la lecture : ${e.getMessage}")
    }
  }
}
