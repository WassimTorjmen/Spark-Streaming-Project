package com.esgi // Déclare le package dans lequel se trouve ce fichier

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord} // Importe les classes nécessaires pour produire des messages Kafka
import java.util.Properties // Pour configurer Kafka
import java.net.{URL, HttpURLConnection} // Pour faire des requêtes HTTP
import scala.io.Source // Pour lire les flux d'entrée (ex : réponses HTTP)
import ujson._ // Pour parser et manipuler du JSON

object ProducerKafka { // Objet principal contenant le programme

  // URL de base de l'API HuggingFace pour récupérer les produits OpenFoodFacts
  val baseUrl = "https://datasets-server.huggingface.co/rows?dataset=openfoodfacts%2Fproduct-database&config=default&split=food"

  def main(args: Array[String]): Unit = { // Point d'entrée du programme

    /* --------- Paramètres --------- */
    //val useAPI      = true 
    //val jsonPath    = "data/food.parquet" 
    val batchLength = 100 // Nombre de produits à récupérer/envoyer par batch
    val maxOffset   = 3808300 // Limite maximale d’offset pour la pagination API
    val topic       = "openfood"
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") // Adresse du serveur Kafka

    /* --------- Configuration Kafka --------- */
    val props = new Properties() // Création d’un objet de configuration
    props.put("bootstrap.servers", bootstrap) // Serveur Kafka

    // Sérialisation de la clé et de la valeur en String
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("max.request.size", "2000000") // Taille maximale de la requête (2 Mo)

    val producer = new KafkaProducer[String, String](props) // Création du producteur Kafka
    println("Producer Kafka - démarrage") 

      var offset = 0 // Offset de départ
      while (offset <= maxOffset) { // Boucle jusqu’à atteindre la limite d’offset
        val url   = s"$baseUrl&offset=$offset&length=$batchLength" // Construit l’URL avec les paramètres
        val batch = fetchBatchFromAPI(url) // Récupère un batch de produits (JSON brut)

        if (batch.nonEmpty) { // Si le batch n’est pas vide
          producer.send(new ProducerRecord(topic, null, batch)) // Envoie le batch dans Kafka
          println(s"Batch offset=$offset envoyé (${batch.length} chars)") // Affiche confirmation + taille du batch

          // Aperçu des données envoyées (limité à 200 caractères)
          val preview = if (batch.length > 200) batch.take(200) + "..." else batch
          println(s"🟢 Batch offset=$offset envoyé  (${batch.length} chars)")
          println(s"   ↳ Aperçu : $preview\n")

          Thread.sleep(2000) // Pause de 2 secondes pour éviter de surcharger kafka
        } else {
          println(s"API vide à offset $offset, arrêt.") // Si aucune donnée reçue, on arrête
        }

        offset += batchLength // Passe au batch suivant
        Thread.sleep(2000) // Petite pause entre chaque requête pour éviter de surcharger l'API
      }

    

    producer.flush() // Force l’envoi de tous les messages en attente
    producer.close() // Ferme proprement le producteur Kafka
    println(" Fin d'envoi - producer Kafka fermé.") // Message de fin
  }

  // Fonction qui récupère un batch JSON depuis l’API
  def fetchBatchFromAPI(url: String): String = {
    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection] // Ouvre une connexion HTTP
      conn.setConnectTimeout(2000)
      conn.setReadTimeout(2000)
      val is   = conn.getInputStream // Ouvre le flux d’entrée de la connexion
      val json = Source.fromInputStream(is).mkString //lire le flux d'entrée et le convertir en String
      is.close()
      json
    } catch {
      case e: Exception =>
        println(s"API error : ${e.getMessage}")
        ""
    }
  }

 
}
