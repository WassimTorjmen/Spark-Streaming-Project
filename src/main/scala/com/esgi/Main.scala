  package com.esgi

  object Main {
    def main(args: Array[String]): Unit = {
      println("🚀 Lancement du Producer depuis Main")
      Producer.main(Array()) // Lancer le producer ici
    }
  }