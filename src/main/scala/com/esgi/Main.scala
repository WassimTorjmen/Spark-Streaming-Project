package com.esgi

object Main {
  def main(args: Array[String]): Unit = {
    println("🚀 Lancement du Producer depuis Main")

    try {
      Producer.main(Array()) // Lance le Producer normalement
      println("🟢 Producer exécuté sans exception.")
    } catch {
      case e: Exception =>
        println(s"❌ Erreur dans le Producer : ${e.getMessage}")
        e.printStackTrace()
    }

    // Empêche le programme de se terminer immédiatement
    println("⏳ Le programme reste actif. Ctrl+C pour quitter.")
    while (true) {
      Thread.sleep(1000)
    }
  }
}
