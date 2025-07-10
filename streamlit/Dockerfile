FROM python:3.11

WORKDIR /app

# Copie des dépendances
COPY requirements.txt .

# Installation des libs
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code Streamlit (app.py)
COPY . .

# Port utilisé par Streamlit
EXPOSE 8501

# Commande de démarrage
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
