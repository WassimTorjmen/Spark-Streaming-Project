package com.esgi

import scala.io.Source
import java.net.{ServerSocket, Socket}
import java.io.PrintWriter
import ujson._

object Producer {

  val baseUrl = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"

  def main(args: Array[String]): Unit = {
    println(" [ProducerSocket] Démarrage...")

    val useAPI = true
    val jsonPath = "data/response.json"
    val batchLength = 100 // nombre de lignes par requête
    val maxOffset = 1000 // pour tester (mets 3808300 plus tard)

    try {
      val serverSocket = new ServerSocket(9999)
      println(" [ProducerSocket] Socket lancé sur le port 9999, en attente d'un client...")

      val socket: Socket = serverSocket.accept()
      println(" [ProducerSocket] Client connecté ! Début du streaming...")

      val out = new PrintWriter(socket.getOutputStream, true)

      if (useAPI) {
        // Pagination sur l'API
        var offset = 0
        while (offset <= maxOffset) {
          val url = s"$baseUrl&offset=$offset&length=$batchLength"
          println(s" [API] Requête : $url")

          val products = fetchRowsFromAPI(url)
          if (products.isEmpty) {
            println(s" [ProducerSocket] Aucun produit trouvé à offset $offset. Arrêt.")
            offset = maxOffset + 1
          } else {
            products.foreach { product =>
              val line = product.render()
              out.println(line)
              println(s"[SEND API] $line")
              Thread.sleep(1000)
            }
            offset += batchLength
          }
        }
      } else {
        // Lecture d'un fichier local JSON
        val products = fetchRowsFromFile(jsonPath)
        products.foreach { product =>
          val line = product.render()
          out.println(line)
          println(s"[SEND FILE] $line")
          Thread.sleep(1000)
        }
      }

      println(" [ProducerSocket] Tous les produits envoyés.")
      while (true) Thread.sleep(1000)

    } catch {
      case e: Exception =>
        println(s" [ProducerSocket] Exception : ${e.getMessage}")
        e.printStackTrace()
    }
  }

  def fetchRowsFromAPI(url: String): Seq[Value] = {
    try {
      val connection = new java.net.URL(url).openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)

      val is = connection.getInputStream
      val content = Source.fromInputStream(is).mkString
      is.close()

      val json = ujson.read(content)
      json("rows").arr.map(_("row"))
    } catch {
      case e: Exception =>
        println(s" [API] Erreur de lecture : ${e.getMessage}")
        Seq.empty
    }
  }

  def fetchRowsFromFile(path: String): Seq[Value] = {
    try {
      val content = Source.fromFile(path).mkString
      val json = ujson.read(content)
      json("rows").arr.map(_("row"))
    } catch {
      case e: Exception =>
        println(s" [FILE] Erreur de lecture : ${e.getMessage}")
        Seq.empty
    }
  }
}
