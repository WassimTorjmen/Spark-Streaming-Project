#!/bin/bash
set -e

# 1. Démarre Kafka en arrière-plan
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
pid=$!

# 2. Attend que le broker réponde sur le port 9092
echo "⏳ waiting for broker…"
while ! nc -z localhost 9092; do sleep 1; done
echo "✅ broker up"

# 3. Crée automatiquement les topics demandés
IFS=',' read -ra TOPICS <<< "${KAFKA_TOPICS:-openfood:1:1}"
for t in "${TOPICS[@]}"; do
  IFS=':' read name part rep <<< "$t"
  echo "⚙️  creating topic $name"
  $KAFKA_HOME/bin/kafka-topics.sh --create \
    --if-not-exists \
    --bootstrap-server localhost:9092 \
    --topic "$name" \
    --partitions "${part:-1}" \
    --replication-factor "${rep:-1}"
done

# 4. Met le broker au premier plan
wait $pid
