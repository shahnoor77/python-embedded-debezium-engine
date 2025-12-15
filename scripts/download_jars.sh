#!/usr/bin/env bash
set -e


# Versions

DEBEZIUM_VERSION="2.5.0.Final"
KAFKA_CONNECT_VERSION="3.6.0"
POSTGRESQL_JDBC_VERSION="42.7.2"


# Target directory

TARGET_DIR="/app/debezium/lib"

echo "Creating directory: $TARGET_DIR"
mkdir -p "$TARGET_DIR"
cd "$TARGET_DIR"


# Debezium 2.5.0.Final

echo "Downloading Debezium jars..."
wget -q https://repo1.maven.org/maven2/io/debezium/debezium-api/${DEBEZIUM_VERSION}/debezium-api-${DEBEZIUM_VERSION}.jar
wget -q https://repo1.maven.org/maven2/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar
wget -q https://repo1.maven.org/maven2/io/debezium/debezium-embedded/${DEBEZIUM_VERSION}/debezium-embedded-${DEBEZIUM_VERSION}.jar
wget -q https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}.jar


# Kafka Connect 3.6.0

echo "Downloading Kafka Connect jars..."
wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CONNECT_VERSION}/kafka-clients-${KAFKA_CONNECT_VERSION}.jar
wget -q https://repo1.maven.org/maven2/org/apache/kafka/connect-api/${KAFKA_CONNECT_VERSION}/connect-api-${KAFKA_CONNECT_VERSION}.jar
wget -q https://repo1.maven.org/maven2/org/apache/kafka/connect-runtime/${KAFKA_CONNECT_VERSION}/connect-runtime-${KAFKA_CONNECT_VERSION}.jar
wget -q https://repo1.maven.org/maven2/org/apache/kafka/connect-json/${KAFKA_CONNECT_VERSION}/connect-json-${KAFKA_CONNECT_VERSION}.jar

# Other Dependencies

echo "Downloading additional dependencies..."
wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar
wget -q https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar
wget -q https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.9/slf4j-simple-2.0.9.jar
wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.3/jackson-databind-2.15.3.jar
wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.3/jackson-core-2.15.3.jar
wget -q https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.3/jackson-annotations-2.15.3.jar

echo " All JARs downloaded successfully into $TARGET_DIR"
