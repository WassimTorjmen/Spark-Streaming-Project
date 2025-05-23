package com.esgi

import java.io.PrintWriter
import java.net.ServerSocket
import org.apache.spark.sql.SparkSession

object Producer {

  def main(args: Array[String]): Unit = {
    val port = 9999
    val filePath = "data/response.json"

    // Création SparkSession
    val spark = SparkSession.builder()
      .appName("Producer")
      .master("local[*]")
      .getOrCreate()

    // Lecture du fichier avec Spark
    val df = spark.read.text(filePath)

    // Création serveur socket classique
    val serverSocket = new ServerSocket(port)
    println(s"Serveur socket lancé sur le port $port, en attente de connexion...")

    val socket = serverSocket.accept()
    println("Client connecté ! Envoi des données en cours...")

    val out = new PrintWriter(socket.getOutputStream, true)

    // Parcours paresseux des lignes du fichier avec toLocalIterator (évite collect())
    val iterator = df.toLocalIterator()

    while (iterator.hasNext) {
      val line = iterator.next().getString(0)
      out.println(line)
      println(s"[SEND] $line")
      Thread.sleep(1000) // envoie une ligne par seconde
    }

    println("Tous les messages ont été envoyés !")
    out.close()
    socket.close()
    serverSocket.close()

    spark.stop()
  }
}