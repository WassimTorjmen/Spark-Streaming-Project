# --------------------------------------------------------------------
#  Étape 1  : installer PostgreSQL 15 à partir des paquets Debian
# --------------------------------------------------------------------
FROM debian:bookworm-slim

# 1. paquets de base + PostgreSQL
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        postgresql-15 postgresql-client-15 && \
    rm -rf /var/lib/apt/lists/*

# 2. petite optimisation : désactiver "fsync" pour les tests (optionnel)
# RUN echo "fsync = off" >> /etc/postgresql/15/main/postgresql.conf

# 3. Copie des scripts d'init
COPY init.sql /docker-entrypoint-initdb.d/init.sql
COPY pg-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/pg-entrypoint.sh

# 4. Variables par défaut
ENV POSTGRES_DB=openfood \
    POSTGRES_USER=ingest \
    POSTGRES_PASSWORD=ingestpwd

VOLUME ["/var/lib/postgresql/data"]
EXPOSE 5432
ENTRYPOINT ["pg-entrypoint.sh"]
