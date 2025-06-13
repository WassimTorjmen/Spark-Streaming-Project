FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ARG KAFKA_VERSION=3.5.1
ARG SCALA_VERSION=2.12
RUN curl -L https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
    | tar -xz -C /opt && \
    mv /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka

WORKDIR /opt/kafka
ENV KAFKA_HOME=/opt/kafka

# fichiers de config + entrypoint
COPY server.properties  config/server.properties
COPY kafka-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9092
ENTRYPOINT ["/entrypoint.sh"]
