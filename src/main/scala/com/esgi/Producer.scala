package com.esgi

import java.io._
import java.net._
import scala.io.Source
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global

object Producer {

  def main(args: Array[String]): Unit = {
    val port = 9999
    val filePath = "data/response.json"

    // Créer le serveur socket
    val serverSocket = new ServerSocket(port)
    println(s"✅ Serveur socket lancé sur le port $port, en attente de connexion...")

    val socket = serverSocket.accept()
    println("🚀 Client connecté ! Envoi des données en cours...")

    val out = new PrintWriter(socket.getOutputStream, true)

    // Lire le fichier ligne par ligne et simuler l'envoi
    for (line <- Source.fromFile(filePath).getLines()) {
      out.println(line)
      println(s"[SEND] $line")
      Thread.sleep(1000) // Simule 1 ligne par seconde
    }

    println("✅ Tous les messages ont été envoyés !")
    out.close()
    socket.close()
    serverSocket.close()
  }
}

