FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

ARG ZK_VERSION=3.9.1
RUN curl -L https://archive.apache.org/dist/zookeeper/zookeeper-${ZK_VERSION}/apache-zookeeper-${ZK_VERSION}-bin.tar.gz \
    | tar -xz -C /opt && \
    mv /opt/apache-zookeeper-${ZK_VERSION}-bin /opt/zookeeper

WORKDIR /opt/zookeeper
ENV ZOO_CFG_DIR=/opt/zookeeper/conf \
    ZOO_DATA_DIR=/data \
    ZOO_PORT=2181

COPY zk-entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 2181
ENTRYPOINT ["/entrypoint.sh"]
