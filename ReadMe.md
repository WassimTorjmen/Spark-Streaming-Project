# 🚀 **Spark Streaming Project — Pipeline de traitement de données en temps réel**

## 📌 **Description**

Spark Streaming Project est une plateforme de traitement de données en temps réel qui collecte, transforme et analyse des données alimentaires provenant de l'API OpenFood. Le projet combine **Apache Spark Streaming** pour le traitement haute performance, **Kafka** pour l'ingestion de flux, **PostgreSQL** pour le stockage persistant, et **Streamlit** pour la visualisation interactive.

Le projet combine **streaming de données**, **architecture microservices**, **traitement distribué** et **déploiement via Docker**. 

---

## 🎯 **Objectif du projet**

* Collecter des données en temps réel depuis l'API OpenFood. 
* Traiter et transformer les flux de données avec Apache Spark Streaming.
* Stocker les données traitées dans PostgreSQL.
* Visualiser les résultats via un dashboard Streamlit interactif.
* Offrir une infrastructure complète, scalable et facilement déployable.

---

## 🧠 **Contexte & Problématique**

L'analyse de données alimentaires à grande échelle nécessite un pipeline robuste capable de :
- Gérer des volumes importants de données en continu.
- Assurer une faible latence entre l'ingestion et l'analyse.
- Offrir une visualisation en temps réel des résultats. 

Spark Streaming Project résout ce problème en :

* **Ingérant automatiquement** les données via Kafka.
* **Traitant les flux** avec Spark Streaming.
* **Persistant les résultats** dans PostgreSQL. 
* **Exposant un dashboard** interactif pour l'analyse.

---

## 🏗️ **Architecture du pipeline**

1. **Producer (Ingestion)**

   * Récupération des données depuis l'API OpenFood. 
   * Publication des messages dans Kafka.
   * Configuration du batch et de l'offset.

2. **Kafka (Message Broker)**

   * Gestion du flux de données en temps réel. 
   * Coordination via ZooKeeper.
   * Garantie de délivrance des messages.

3. **Consumer (Traitement Spark)**

   * Lecture des messages Kafka.
   * Transformation et nettoyage des données.
   * Agrégations et calculs en temps réel. 
   * Écriture dans PostgreSQL.

4. **PostgreSQL (Stockage)**

   * Persistance des données traitées.
   * Base de données `openfood`.
   * Scripts d'initialisation automatiques.

5. **Streamlit (Visualisation)**

   * Dashboard interactif. 
   * Graphiques et métriques en temps réel.
   * Connexion directe à PostgreSQL.

6. **Docker & Infrastructure**

   * Orchestration multi-conteneurs via Docker Compose.
   * Réseaux isolés (backend-net, frontend-net). 
   * Volumes persistants pour les données.

---

## 🛠️ **Technologies utilisées**

**Langages :**

* Scala (54. 6%)
* Python (29.3%)
* Docker (16.1%)

**Traitement de données :**

* Apache Spark 3.5.5
* Spark Streaming
* Spark SQL

**Message Broker :**

* Apache Kafka
* Apache ZooKeeper

**Base de données :**

* PostgreSQL 15

**Visualisation :**

* Streamlit
* Plotly / Matplotlib

**Infra & DevOps :**

* Docker & Docker Compose
* Maven
* Git

---

## 📚 **Compétences mobilisées**

### **Big Data & Streaming**

* Apache Spark Streaming
* Micro-batching
* Fenêtrage temporel (sliding/tumbling windows)
* Stateful processing
* Kafka integration

### **Data Engineering**

* Ingestion de données API
* ETL en temps réel
* Structuration SQL
* Pipelines de traitement distribués

### **Backend & DevOps**

* Architecture microservices
* Containerisation Docker
* Orchestration Docker Compose
* Configuration infrastructure

---

## 🚀 **Fonctionnalités principales**

* Collecte automatique de données depuis l'API OpenFood. 
* Traitement en temps réel avec Spark Streaming.
* Stockage persistant dans PostgreSQL.
* Dashboard Streamlit interactif.
* Infrastructure complète containerisée. 
* Scalabilité horizontale des workers Spark.

---

## 📂 **Structure du projet**

```
Spark-Streaming-Project/
├── src/
│   └── main/
│       └── scala/
│           └── com/esgi/
│               ├── Producer/          # Producer Kafka
│               │   └── Dockerfile
│               └── Consumer/          # Consumer Spark Streaming
│                   └── Dockerfile
├── streamlit/                         # Application Streamlit
│   ├── app.py                         # Dashboard principal
│   ├── Dockerfile
│   └── requirements.txt
├── Infra/                             # Configuration infrastructure
│   ├── kafka/                         # Configuration Kafka
│   ├── postgres/                      # Scripts SQL d'initialisation
│   │   └── init.sql
│   ├── zookeeper/                     # Configuration ZooKeeper
│   └── docker-compose.yml             # Compose infrastructure
├── libs/                              # Bibliothèques partagées
├── data/                              # Données locales
├── pom.xml                            # Configuration Maven
├── docker-compose.yml                 # Orchestration principale
├── Dockerfile
└── README.md
```

---

## ▶️ **Installation & Exécution**

### 1.  Cloner le projet

```bash
git clone https://github.com/WassimTorjmen/Spark-Streaming-Project. git
cd Spark-Streaming-Project
```

### 2. Démarrer l'environnement complet

```bash
docker-compose up -d
```

### 3. Vérifier le statut des services

```bash
docker-compose ps
```

### 4. Voir les logs

```bash
# Logs du producer
docker-compose logs -f producer

# Logs du consumer
docker-compose logs -f consumer

# Logs Kafka
docker-compose logs -f kafka
```

### 5.  Accéder au dashboard Streamlit

👉 [http://localhost:8501](http://localhost:8501)

### 6. Accéder à PostgreSQL

```bash
psql -h localhost -p 5433 -U ingest -d openfood
# Mot de passe : ingestpwd
```

---

## 🏗️ **Architecture Docker**

```
┌─────────────────────────────────────────────────────────────┐
│              Docker Compose Orchestration                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐  ┌──────────────────────┐            │
│  │ ZooKeeper        │  │ Kafka                │            │
│  │ - Port 2181      │◄─│ - Port 9092          │            │
│  └──────────────────┘  └──────────────────────┘            │
│                              │                              │
│           ┌──────────────────┼──────────────────┐          │
│           ▼                  │                  ▼          │
│  ┌──────────────────┐        │        ┌──────────────────┐ │
│  │ Producer         │────────┘        │ Consumer (Spark) │ │
│  │ - API OpenFood   │                 │ - Spark Streaming│ │
│  └──────────────────┘                 └──────────────────┘ │
│                                              │              │
│                                              ▼              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ PostgreSQL                                           │  │
│  │ - Port 5433                                          │  │
│  │ - Base: openfood                                     │  │
│  └──────────────────────────────────────────────────────┘  │
│                              │                              │
│                              ▼                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Streamlit Dashboard                                  │  │
│  │ - Port 8501                                          │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## ⚙️ **Configuration**

### Variables d'environnement Producer

| Variable | Description | Défaut |
|----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Adresse Kafka | `kafka:9092` |
| `USE_API` | Utiliser l'API OpenFood | `true` |
| `BATCH_LENGTH` | Taille du batch | `100` |
| `MAX_OFFSET` | Offset maximum | `3808300` |

### Variables d'environnement Consumer

| Variable | Description | Défaut |
|----------|-------------|--------|
| `KAFKA_BOOTSTRAP_SERVERS` | Adresse Kafka | `kafka:9092` |
| `PG_URL` | URL PostgreSQL | `jdbc:postgresql://postgres:5432/openfood` |
| `PG_USER` | Utilisateur PostgreSQL | `ingest` |
| `PG_PWD` | Mot de passe PostgreSQL | `ingestpwd` |
| `CHECKPOINT_PATH` | Chemin checkpoint Spark | `/checkpoint/generic` |

---

## 📊 **Exemple de flux de données**

### Input (données API OpenFood)

```json
{
  "product_name": "Nutella",
  "brands": "Ferrero",
  "categories": "Pâtes à tartiner",
  "nutriscore_grade": "e",
  "energy_100g": 2252
}
```

### Traitement Spark

```scala
// Lecture depuis Kafka
val stream = spark.readStream
  .format("kafka")
  .option("subscribe", "openfood-topic")
  .load()

// Transformation et agrégation
val processed = stream
  .select(from_json(col("value"), schema).as("data"))
  .groupBy("data.nutriscore_grade")
  . count()
```

### Résultat PostgreSQL

| nutriscore_grade | count |
|------------------|-------|
| a | 15234 |
| b | 28451 |
| c | 34123 |
| d | 21098 |
| e | 12456 |

---

## 🧪 **Tests**

### Compilation Maven

```bash
mvn clean install
mvn package
```

### Test de connectivité Kafka

```bash
docker exec -it kafka kafka-topics. sh --list --bootstrap-server localhost:9092
```

### Test PostgreSQL

```bash
docker exec -it postgres psql -U ingest -d openfood -c "SELECT COUNT(*) FROM products;"
```

---

## 📈 **Performances et optimisations**

* **Partitionnement Kafka** : Distribution optimale des messages. 
* **Checkpointing Spark** : Récupération en cas de défaillance.
* **Batch interval tuning** : Équilibre latence vs throughput.
* **Connection pooling** : Optimisation des connexions PostgreSQL. 
* **Volumes Docker** : Persistance des données et logs.

---

## 📞 **Support**

Pour toute question, ouvrez une [issue](https://github.com/WassimTorjmen/Spark-Streaming-Project/issues). 

---

## 👤 **Auteur**

**Wassim Torjmen**

---

**Dernière mise à jour** : Décembre 2025
