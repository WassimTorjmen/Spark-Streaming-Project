##############################################################################
# Étape 1 : build (Maven + JDK 11 déjà installés)                            #
##############################################################################
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn -q dependency:go-offline -Dmaven.repo.local=/root/.m2

# COPIE uniquement le code du consumer (+ éventuellement Main.scala)
COPY src/main/scala/com/esgi/Consumer              src/main/scala/com/esgi/Consumer

RUN mvn -q package -Dmaven.test.skip=true -Dmaven.repo.local=/root/.m2


##############################################################################
# Étape 2 : runtime - JRE + Spark en local                                   #
##############################################################################
FROM eclipse-temurin:11-jre
WORKDIR /app

# 1. Java runtime déjà présent
# 2. Installer Spark binaire
ARG SPARK_VERSION=3.5.0
RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# 3. Copier le jar
COPY --from=builder /build/target/spark-streaming-project-1.0-SNAPSHOT.jar consumer.jar

# 4. Variables env
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    CHECKPOINT_PATH=/checkpoint/generic 

ENTRYPOINT ["spark-submit",\
            "--class","com.esgi.ConsumerKafka",\
            "--master","local[*]",\
            "/app/consumer.jar"]