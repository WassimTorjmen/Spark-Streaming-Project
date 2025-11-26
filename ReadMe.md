# Spark Streaming Project

## 📋 À propos du projet

**Spark Streaming Project** est une application de traitement de données en temps réel utilisant Apache Spark.  Ce projet combine la puissance de **Scala** (54.6%) pour le traitement de données haute performance, **Python** (29.3%) pour l'orchestration et l'analyse, et **Docker** (16.1%) pour la containerisation et le déploiement.

## 🚀 Caractéristiques principales

### ⚡ Traitement de données en temps réel avec Apache Spark Streaming

Spark Streaming permet de traiter des flux de données continus avec très faible latence :

- **Micro-batching** : Divise le flux de données en petits lots pour un traitement efficace
- **Basse latence** : Latence de quelques secondes pour les résultats
- **Haute throughput** : Traite des millions d'événements par seconde
- **Stateful processing** : Maintient l'état entre les micro-batches pour les agrégations complexes
- **Support multi-sources** : 
  - Kafka, Kinesis, Flume (sources natives)
  - TCP sockets
  - Sources personnalisées via l'API DStream
- **Garanties de délivrance** :
  - At-least-once semantics
  - Exactly-once pour certaines opérations
  - Récupération automatique en cas de défaillance

### Cas d'usage supportés

- **Agrégation en temps réel** : Calcul de moyennes, sommes, comptages continus
- **Détection d'anomalies** : Identification de patterns anormaux dans les flux
- **Join de flux** : Corrélation de données provenant de multiples sources
- **Fenêtrage temporel** : Sliding windows, tumbling windows
- **Stateful transformations** : MapWithState, UpdateStateByKey pour les opérations complexes

```scala
// Exemple : Agrégation sur fenêtre glissante
val windowedWordCounts = words
  .map(word => (word, 1))
  .reduceByKeyAndWindow(
    (a: Int, b: Int) => a + b,
    Seconds(60),      // Fenêtre de 60 secondes
    Seconds(10)       // Slide toutes les 10 secondes
  )
```

---

### 🐍 Scripts Python pour l'orchestration et l'analyse

Une couche Python complète pour compléter Scala :

#### **Orchestration des jobs Spark**
- Gestion des workflows Spark depuis Python
- Soumission dynamique des jobs avec pyspark
- Configuration flexible des paramètres
- Gestion des dépendances et des étapes

#### **Traitement et nettoyage des données**
- **Pandas** : Manipulation et transformation des données
- **NumPy** : Opérations numériques avancées
- **PySpark DataFrames** : Traitement parallèle de grandes volumes
- Nettoyage, validation et normalisation des données

#### **Analyse statistique**
- **SciPy** : Statistiques avancées
- **Scikit-learn** : Machine Learning (clustering, classification, régression)
- Analyse exploratoire des données (EDA)
- Rapports statistiques automatisés

#### **Visualisation et monitoring**
- Dashboards interactifs avec **Streamlit**
- Graphiques en temps réel avec **Plotly/Matplotlib**
- Métriques de performance et logs
- Alertes et notifications

```python
# Exemple : Pipeline d'analyse Python
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("AnalysisPipeline").getOrCreate()

# Charger et traiter les données
df = spark.read.parquet("s3://bucket/data")
result = df.filter(F.col("value") > 100) \
    .groupBy("category") \
    .agg(F.avg("amount").alias("avg_amount"))

# Visualiser avec Streamlit
import streamlit as st
st.dataframe(result. toPandas())
```

#### **Intégration avec des services externes**
- APIs REST pour récupérer/envoyer des données
- Connexion à des bases de données (PostgreSQL, MongoDB, etc.)
- Interaction avec le cloud (AWS S3, Azure Blob, GCP)
- Webhooks et notifications

---

### 🐳 Infrastructure Docker pour déploiement facile

Une architecture containerisée complète pour simplifier le déploiement :

#### **Avantages du déploiement Docker**
- ✅ **Reproductibilité** : Même environnement sur tous les serveurs
- ✅ **Isolation** : Services indépendants sans conflits de dépendances
- ✅ **Scalabilité** : Lancer plusieurs instances facilement
- ✅ **Portabilité** : Fonctionne sur Windows, Mac, Linux, Cloud
- ✅ **Versioning** : Tracer les versions d'infrastructure

#### **Architecture multi-conteneurs**

```
┌─────────────────────────────────────────────────────────┐
│            Docker Compose Orchestration                 │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────┐  ┌──────────────────────┐        │
│  │ Spark Master     │  │ Spark Worker (x2+)   │        │
│  │ - Port 8080      │  │ - Dynamic ports      │        │
│  │ - REST API       │  │ - Auto-scaling       │        │
│  └──────────────────┘  └──────────────────────┘        │
│           │                     │                      │
│           └─────────┬───────────┘                       │
│                     │                                   │
│  ┌──────────────┐   │   ┌──────────────────────┐       │
│  │ Kafka/Data   │◄──┴──►│ Streamlit Dashboard  │       │
│  │ Source       │       │ - Port 8501          │       │
│  └──────────────┘       └──────────────────────┘       │
│           │                                            │
│           ▼                                            │
│  ┌──────────────────────────────┐                      │
│  │ PostgreSQL/MongoDB           │                      │
│  │ - Persistence                │                      │
│  │ - Port 5432/27017           │                      │
│  └──────────────────────────────┘                      │
│                                                        │
└─────────────────────────────────────────────────────────┘
```

#### **Configurations Docker disponibles**

**Services inclus :**
- **Spark Master** : Coordonne les jobs et les ressources
- **Spark Workers** : Exécutent les tâches en parallèle
- **Kafka** (optionnel) : Source de données en temps réel
- **PostgreSQL/MongoDB** : Stockage persistant
- **Streamlit** : Interface web interactive
- **JupyterLab** (optionnel) : Développement et prototypage

**Avantages de cette approche :**
- Tous les services démarrent automatiquement
- Networking automatique entre conteneurs
- Partage de volumes pour persistence
- Logs centralisés
- Facile de scale les workers

```yaml
# Exemple docker-compose.yml simplifié
version: '3.8'
services:
  spark-master:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=master
    ports:
      - "8080:8080"
  
  spark-worker:
    image: bitnami/spark:latest
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
    depends_on:
      - spark-master
    deploy:
      replicas: 3  # Lancer 3 workers
```

---

### 📊 Architecture modulaire et scalable

Une structure de code bien organisée pour la maintenabilité :

#### **Modularité**
- **Séparation des responsabilités** : Chaque module a une fonction clairement définie
- **Réutilisabilité** : Code packagé dans des bibliothèques (`libs/`)
- **Testabilité** : Code découplé facile à tester unitairement
- **Extensibilité** : Ajouter facilement de nouvelles fonctionnalités

#### **Scalabilité horizontale**
- Ajouter des Spark Workers sans modification du code
- Distribution automatique des charges
- Parallélisation des transformations
- RDD/DataFrame partitionnés efficacement

#### **Scalabilité verticale**
- Augmenter les ressources (CPU, RAM) par conteneur
- Configuration flexible des paramètres Spark
- Tuning des partitions et du batch interval

---

### 🔄 Support multi-langage (Scala + Python)

Exploite les forces de deux langages :

#### **Pourquoi Scala ?**
- **Performance** : Compilé, typé statiquement (plus rapide que Python)
- **Naturellement parallèle** : Immutabilité, pas de race conditions
- **DSL expressif** : API Spark très naturelle en Scala
- **Type-safe** : Erreurs détectées à la compilation

#### **Pourquoi Python ?**
- **Développement rapide** : Syntaxe simple et productive
- **Écosystème data science** : Pandas, NumPy, SciPy, Scikit-learn
- **Data scientists friendly** : Langage préféré des analystes
- **Intégrations faciles** : APIs, webhooks, services cloud

#### **Interopérabilité**
- Scala pour les jobs Spark haute-performance
- Python pour l'orchestration et la visualisation
- Communication via fichiers (Parquet), API REST, ou messages
- Partage de données via DataFrames

```scala
// Job Scala haute-performance
object StreamingJob {
  def main(args: Array[String]) {
    val ssc = new StreamingContext(sc, Seconds(1))
    val dstream = ssc.socketTextStream("localhost", 9999)
    dstream.map(_.split(" "))
      .map(words => (words(0), 1))
      .reduceByKey(_ + _)
      .saveAsTextFiles("hdfs://path/output")
    ssc.start()
  }
}
```

```python
# Orchestration Python
import subprocess
import pandas as pd

# Soumettre le job Scala
subprocess.run([
    "spark-submit",
    "--class", "com.example.StreamingJob",
    "target/spark-project.jar"
])

# Analyser les résultats
results = pd.read_parquet("hdfs://path/output")
print(results.describe())
```

---

## 📦 Stack technologique

| Technologie | Pourcentage | Utilisation |
|---|---|---|
| **Scala** | 54.6% | Traitement de données et logique métier |
| **Python** | 29.3% | Scripts d'orchestration et visualisation |
| **Docker** | 16.1% | Containerisation et déploiement |

---

## 📁 Structure du projet

```
Spark-Streaming-Project/
├── src/
│   ├── main/
│   │   ├── scala/              # Code Scala principal
│   │   │   └── com/example/
│   │   │       ├── streaming/  # Jobs Spark Streaming
│   │   │       ├── utils/      # Utilitaires
│   │   │       └── config/     # Configuration
│   │   └── resources/          # Fichiers de config (YAML, properties)
│   └── test/
│       ├── scala/              # Tests unitaires Scala
│       └── python/             # Tests Python
├── streamlit/                  # Applications Streamlit
│   ├── app.py                  # Dashboards interactifs
│   └── components/             # Composants réutilisables
├── Infra/                      # Configuration infrastructure
│   ├── terraform/              # Infrastructure as Code
│   └── ansible/                # Configuration management
├── libs/                       # Bibliothèques partagées
│   ├── common/                 # Code commun
│   └── validators/             # Validateurs
├── pom.xml                     # Configuration Maven
├── docker-compose.yml          # Orchestration Docker
├── Dockerfile                  # Image Docker personnalisée
└── ReadMe.md                   # Documentation
```

---

## 🛠️ Installation et mise en place

### Prérequis

- Docker et Docker Compose (v1.29+)
- Apache Spark 3.x
- Scala 2.12+
- Python 3.8+
- Maven 3.6+
- Git

### Démarrage rapide avec Docker Compose

```bash
# Cloner le repository
git clone https://github.com/WassimTorjmen/Spark-Streaming-Project.git
cd Spark-Streaming-Project

# Démarrer l'environnement complet
docker-compose up -d

# Vérifier le status
docker-compose ps

# Voir les logs
docker-compose logs -f spark-master
```

### Installation locale

```bash
# Installer les dépendances Maven
mvn clean install

# Compiler le projet
mvn package

# Exécuter un job Spark
spark-submit --class com.example.streaming.StreamingApp \
  target/spark-streaming-1.0.jar
```

---

## 📊 Performances et optimisations

- **Partitionnement intelligent** : Optimisation du nombre de partitions
- **Batch interval tuning** : Équilibre latence vs throughput
- **RDD caching** : Optimisation des opérations répétées
- **Compression de données** : Réduction de l'utilisation mémoire
- **Parallélisme adaptatif** : Auto-tuning des ressources

---

## 📞 Support

Pour toute question, ouvrez une [issue](https://github.com/WassimTorjmen/Spark-Streaming-Project/issues).

---

**Dernière mise à jour** : 26 novembre 2025
