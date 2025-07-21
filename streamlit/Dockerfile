# Utilise une image officielle de Python 3.11 comme image de base.
# Cette image contient déjà Python installé et est adaptée pour les applications Python.
FROM python:3.11

# Définit le répertoire de travail dans le conteneur.
# Toutes les commandes suivantes (RUN, COPY…) seront exécutées dans /app.
WORKDIR /app

# Copie le fichier des dépendances dans l’image Docker.
# Ce fichier contient toutes les bibliothèques Python nécessaires (dont Streamlit).
COPY requirements.txt .

# Installe les dépendances à partir de requirements.txt sans les mettre en cache
# (cela permet d’alléger l’image finale).
RUN pip install --no-cache-dir -r requirements.txt

# Copie **tous les fichiers du dossier local (code source)** dans le répertoire /app du conteneur.
# Cela inclut `app.py` et tous les autres fichiers nécessaires à Streamlit.
COPY . .

# Expose le port 8501, qui est le port par défaut utilisé par Streamlit.
# Cela permet à l’application d’être accessible depuis l’extérieur du conteneur.
EXPOSE 8501

# Commande à exécuter lors du démarrage du conteneur :
# Elle lance l'application Streamlit sur le port 8501 et accessible via 0.0.0.0
# (pour qu’elle soit accessible depuis l’extérieur, pas juste en localhost).
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
