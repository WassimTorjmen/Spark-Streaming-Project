#!/bin/bash
set -e

# si aucune base n'existe encore, initdb
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  echo "📦 Initialisation cluster PostgreSQL..."
  su - postgres -c "/usr/lib/postgresql/15/bin/initdb -D $PGDATA"
fi

# lance Postgres (en arrière-plan) pour exécuter les scripts .sql
su - postgres -c "/usr/lib/postgresql/15/bin/pg_ctl -D $PGDATA -o \"-c listen_addresses=''\" -w start"

# exécute *.sql présents dans /docker-entrypoint-initdb.d
for f in /docker-entrypoint-initdb.d/*.sql; do
  [ -f "$f" ] || continue
  echo "▶️  running $f"
  su - postgres -c "psql -v ON_ERROR_STOP=1 -f $f"
done

# arrêt du serveur temporaire
su - postgres -c "/usr/lib/postgresql/15/bin/pg_ctl -D $PGDATA -m fast -w stop"

# démarre Postgres au premier plan (production)
exec su - postgres -c "/usr/lib/postgresql/15/bin/postgres -D $PGDATA \
      -c listen_addresses='*' \
      -c max_connections=100"
