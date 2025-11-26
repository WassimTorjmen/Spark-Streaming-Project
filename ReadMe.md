# 🚀 Spark Streaming Project - Open Food Facts Analytics

Pipeline de traitement de données en temps réel utilisant **Apache Kafka**, **Apache Spark Structured Streaming**, **PostgreSQL** et **Streamlit** pour analyser les données Open Food Facts.

![Architecture](https://img.shields.io/badge/Architecture-Microservices-blue)
![Scala](https://img.shields.io/badge/Scala-2.12-red)
![Spark](https://img.shields.io/badge/Spark-3.5.5-orange)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 📋 Table des matières

- [Description](#-description)
- [Architecture](#-architecture)
- [Technologies utilisées](#-technologies-utilisées)
- [Prérequis](#-prérequis)
- [Installation et démarrage](#-installation-et-démarrage)
- [Structure du projet](#-structure-du-projet)
- [Transformations et agrégations](#-transformations-et-agrégations)
- [Dashboard Streamlit](#-dashboard-streamlit)
- [Configuration](#-configuration)
- [Auteurs](#-auteurs)

---

## 📖 Description

Ce projet implémente un **pipeline de streaming de données** complet qui :

1. **Collecte** les données produits depuis l'API Open Food Facts (via HuggingFace Datasets)
2. **Transmet** les données en temps réel via Apache Kafka
3. **Transforme** et agrège les données avec Spark Structured Streaming
4. **Stocke** les résultats dans PostgreSQL
5. **Visualise** les insights via un dashboard Streamlit interactif

---

## 🏗 Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────────┐
│   Open Food     │────▶│    Kafka    │────▶│  Spark Consumer │
│   Facts API     │     │   Broker    │     │   (Streaming)   │
└─────────────────┘     └─────────────┘     └────────┬────────┘
                              ▲                      │
                              │                      ▼
                     ┌────────┴────────┐     ┌──────────────┐
                     │   Zookeeper     │     │  PostgreSQL  │
                     └─────────────────┘     └──────┬───────┘
                                                    │
                                                    ▼
                                            ┌──────────────┐
                                            │  Streamlit   │
                                            │  Dashboard   │
                                            └──────────────┘
```

---

## 🛠 Technologies utilisées

| Technologie | Version | Rôle |
|-------------|---------|------|
| **Scala** | 2.12.18 | Langage de développement |
| **Apache Spark** | 3.5.5 | Traitement de données en streaming |
| **Apache Kafka** | 3.5.1 | Message broker |
| **Zookeeper** | 3.9 | Coordination Kafka |
| **PostgreSQL** | 15 | Base de données relationnelle |
| **Streamlit** | latest | Dashboard interactif |
| **Docker Compose** | 3.9 | Containerisation et orchestration |

---

## ✅ Prérequis

- **Docker** et **Docker Compose** installés sur votre machine
- **8 Go de RAM** minimum recommandés
- **Ports disponibles** : 2181 (Zookeeper), 9092 (Kafka), 5433 (PostgreSQL), 8501 (Streamlit)

---

## 🚀 Installation et démarrage

### 1. Cloner le repository

```bash
git clone https://github.com/WassimTorjmen/Spark-Streaming-Project.git
cd Spark-Streaming-Project
```

### 2. Lancer l'infrastructure

```bash
docker compose up --build -d
```

### 3. Vérifier que tous les services sont actifs

```bash
docker compose ps
```

### 4. Accéder au dashboard

Ouvrez votre navigateur à l'adresse : **http://localhost:8501**

### 5. Arrêter les services

```bash
docker compose down
```

Pour supprimer également les volumes (données persistantes) :

```bash
docker compose down -v
```

---

## 📁 Structure du projet

```
Spark-Streaming-Project/
├── docker-compose.yml          # Orchestration des services
├── pom.xml                     # Configuration Maven (dépendances Scala/Spark)
├── Dockerfile                  # Image Docker principale
│
├── src/main/scala/com/esgi/
│   ├── Main.scala              # Point d'entrée principal
│   ├── Producer/
│   │   ├── Producer.scala      # Producteur Kafka (récupère les données API)
│   │   └── Dockerfile
│   └── Consumer/
│       ├── Consumer.scala      # Consommateur Spark Streaming
│       └── Dockerfile
│
├── Infra/
│   ├── kafka/
│   │   ├── Dockerfile
│   │   └── server.properties
│   ├── postgres/
│   │   └── init.sql            # Script d'initialisation des tables
│   └── docker-compose.yml
│
├── streamlit/
│   ├── app.py                  # Application Streamlit
│   ├── Dockerfile
│   └── requirements.txt
│
└── data/                       # Données locales (optionnel)
```

---

## 🔄 Transformations et agrégations

Le consumer Spark effectue les transformations suivantes :

| Table | Description |
|-------|-------------|
| `nutriscore_counts` | Répartition des produits par Nutriscore (A, B, C, D, E) |
| `category_counts` | Nombre de produits par catégorie principale |
| `brand_counts` | Nombre de produits par marque |
| `packaging_distribution` | Répartition par type d'emballage |
| `top_additive_products` | Top 10 des produits avec le plus d'additifs |
| `nova_group_classification` | Classification NOVA (niveau de transformation) |
| `top_sugary_products_by_category` | Produits les plus sucrés par catégorie |

---

## 📊 Dashboard Streamlit

Le dashboard offre les visualisations suivantes :

- **📈 Graphique en barres** : Répartition des produits par Nutriscore
- **🥧 Diagramme circulaire** : Top 9 des catégories principales
- **📊 Graphique en barres** : Top 10 des marques
- **🔵 Donut chart** : Types d'emballage les plus courants
- **📋 Tableau** : Produits avec le plus d'additifs
- **📉 Graphique NOVA** : Classification par niveau de transformation

---

## ⚙️ Configuration

### Variables d'environnement

| Variable | Valeur par défaut | Description |
|----------|-------------------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Adresse du broker Kafka |
| `BATCH_LENGTH` | `100` | Nombre de produits par batch |
| `MAX_OFFSET` | `3808300` | Offset maximum pour l'API |
| `PG_URL` | `jdbc:postgresql://postgres:5432/openfood` | URL PostgreSQL |
| `PG_USER` | `ingest` | Utilisateur PostgreSQL |
| `PG_PWD` | `ingestpwd` | Mot de passe PostgreSQL |
| `CHECKPOINT_PATH` | `/checkpoint/generic` | Chemin des checkpoints Spark |

### Ports exposés

| Service | Port |
|---------|------|
| Zookeeper | 2181 |
| Kafka | 9092 |
| PostgreSQL | 5433 (mappé vers 5432 interne) |
| Streamlit | 8501 |

---

## 🔧 Commandes utiles

```bash
# Voir les logs d'un service spécifique
docker compose logs -f producer
docker compose logs -f consumer
docker compose logs -f streamlit

# Accéder à PostgreSQL
docker exec -it postgres psql -U ingest -d openfood

# Vérifier les tables (dans psql)
# \dt

# Voir les données Nutriscore (dans psql)

# SELECT * FROM nutriscore_counts;

# Reconstruire un service spécifique
docker compose up --build -d consumer
```

---

## 🐛 Dépannage

| Problème | Solution |
|----------|----------|
| Kafka ne démarre pas | Vérifiez que Zookeeper est actif : `docker compose logs zookeeper` |
| Consumer n'écrit pas dans PostgreSQL | Vérifiez les credentials et que la base `openfood` existe |
| Streamlit affiche "Connexion échouée" | Attendez que PostgreSQL soit initialisé (30-60 secondes) |
| Mémoire insuffisante | Augmentez la RAM Docker ou réduisez `BATCH_LENGTH` |

---

## 👥 Auteurs

- **Wassim Torjmen** - *Développeur principal*

---

## 📄 Licence

Ce projet est sous licence MIT.

---

## 🙏 Remerciements

- [Open Food Facts](https://world.openfoodfacts.org/) pour les données alimentaires ouvertes
- [HuggingFace Datasets](https://huggingface.co/datasets/openfoodfacts/product-database) pour l'API de données
