##############################################################################
# Étape 1 : build  (Maven + JDK 11 déjà pré-installés)                       #
##############################################################################
FROM maven:3.9-eclipse-temurin-11 AS builder
WORKDIR /build

# 1. pom.xml à la racine
COPY pom.xml .

# 2. Pré-téléchargement des dépendances
RUN mvn -q dependency:go-offline -Dmaven.repo.local=/root/.m2

# 3. Copier les sources utiles au Producer
COPY src/main/scala/com/esgi/Producer          src/main/scala/com/esgi/Producer

# 4. Compilation jar (fat-jar)
RUN mvn -q package -Dmaven.test.skip=true -Dmaven.repo.local=/root/.m2
RUN ls -lh target

##############################################################################
# Étape 2 : image runtime (JRE 11 only)                                      #
##############################################################################
FROM eclipse-temurin:11-jre
WORKDIR /app

# Jar produit
#COPY --from=builder /build/target/*-shaded.jar /app/producer.jar
COPY --from=builder /build/target/*-shaded.jar /app/producer.jar


ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    USE_API=true \
    BATCH_LENGTH=100 \
    MAX_OFFSET=3808300 \
    JAVA_TOOL_OPTIONS="-Xms256m -Xmx512m"

ENTRYPOINT ["java","-jar","/app/producer.jar"]
