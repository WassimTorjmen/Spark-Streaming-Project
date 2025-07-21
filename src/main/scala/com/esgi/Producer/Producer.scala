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
    val useAPI      = true // Si true, on lit les données depuis l’API ; sinon depuis un fichier local
    val jsonPath    = "data/food.parquet" // Chemin du fichier local (si useAPI = false)
    val batchLength = 100 // Nombre de produits à récupérer/envoyer par batch
    val maxOffset   = 3808300 // Limite maximale d’offset pour la pagination API
    val topic       = "openfood" // Nom du topic Kafka
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092") // Adresse du serveur Kafka

    /* --------- Configuration Kafka --------- */
    val props = new Properties() // Création d’un objet de configuration
    props.put("bootstrap.servers", bootstrap) // Serveur Kafka

    // Sérialisation de la clé et de la valeur en String
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("max.request.size", "2000000") // Limite de taille d’une requête Kafka (en octets)

    val producer = new KafkaProducer[String, String](props) // Création du producteur Kafka
    println("Producer Kafka – démarrage") // Message d'information

    // Si on utilise l’API
    if (useAPI) {
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

          Thread.sleep(2000) // Pause de 2 secondes pour éviter de surcharger l’API
        } else {
          println(s"API vide à offset $offset, arrêt.") // Si aucune donnée reçue, on arrête
        }

        offset += batchLength // Passe au batch suivant
        Thread.sleep(2000) // Petite pause entre chaque requête
      }

    } else {
      // Si on lit depuis un fichier local
      fetchBatchesFromFile(jsonPath).foreach { batch =>
        producer.send(new ProducerRecord(topic, null, batch)) // Envoie chaque batch depuis le fichier
        println(s"Batch fichier envoyé (${batch.length} chars)") // Affiche confirmation
        Thread.sleep(1000) // Pause entre les envois
      }
    }

    producer.flush() // Force l’envoi de tous les messages en attente
    producer.close() // Ferme proprement le producteur Kafka
    println(" Fin d’envoi – producer Kafka fermé.") // Message de fin
  }

  // Fonction qui récupère un batch JSON depuis l’API
  def fetchBatchFromAPI(url: String): String = {
    try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection] // Ouvre la connexion HTTP
      conn.setConnectTimeout(2000) // Timeout de connexion
      conn.setReadTimeout(2000) // Timeout de lecture

      val is   = conn.getInputStream // Récupère le flux de réponse
      val json = Source.fromInputStream(is).mkString // Lit la réponse JSON en chaîne de caractères
      is.close() // Ferme le flux
      json // Retourne le JSON
    } catch {
      case e: Exception =>
        println(s" API error : ${e.getMessage}") // Affiche l'erreur en cas d’échec
        "" // Retourne une chaîne vide
    }
  }

  // Fonction qui lit un fichier local JSON et le découpe en batches de 100 produits
  def fetchBatchesFromFile(path: String): Vector[String] = {
    try {
      val raw   = Source.fromFile(path).mkString // Lit tout le fichier en chaîne
      val root  = ujson.read(raw) // Parse le contenu JSON
      val rows  = root("rows").arr.map(_("row")) // Récupère les lignes (produits)

      // Groupe les lignes par 100, puis les convertit en JSON
      rows.grouped(100).map(g => ujson.Arr(g: _*).render()).toVector
    } catch {
      case e: Exception =>
        println(s" File error : ${e.getMessage}") // Affiche une erreur en cas d’échec
        Vector.empty // Retourne un vecteur vide
    }
  }
}
