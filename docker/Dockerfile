# 1. BASE IMAGE: Using Python 3.10 slim
FROM python:3.10-slim

# Install system dependencies
# 1: Install Java 21 LTS (The version this Debian release supports)
# 2: Use the correct MariaDB/MySQL dev package
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl wget \
    libmariadb-dev \
    # Using the readily available Java 21 LTS package
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment for Java 21
ENV JAVA_HOME="/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

# 2. DOWNLOAD DEBEZIUM (2.5.0.Final) & KAFKA CONNECT (3.6.0)
# We must align with Java 21, so we revert versions back to the modern suite.
ENV DEBEZIUM_VERSION=2.5.0.Final
ENV KAFKA_CONNECT_VERSION=3.6.0
ENV POSTGRESQL_JDBC_VERSION=42.7.2

RUN mkdir -p /app/debezium/lib && cd /app/debezium/lib && \
    # Debezium 2.5.0.Final components
    wget https://repo1.maven.org/maven2/io/debezium/debezium-api/${DEBEZIUM_VERSION}/debezium-api-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-core/${DEBEZIUM_VERSION}/debezium-core-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-embedded/${DEBEZIUM_VERSION}/debezium-embedded-${DEBEZIUM_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}.jar && \
    \
    # Kafka Connect 3.6.0 components
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CONNECT_VERSION}/kafka-clients-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-api/${KAFKA_CONNECT_VERSION}/connect-api-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-runtime/${KAFKA_CONNECT_VERSION}/connect-runtime-${KAFKA_CONNECT_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/connect-json/${KAFKA_CONNECT_VERSION}/connect-json-${KAFKA_CONNECT_VERSION}.jar && \
    \
    # Other Dependencies (JDBC Driver, Logging, Jackson)
    wget https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_JDBC_VERSION}/postgresql-${POSTGRESQL_JDBC_VERSION}.jar && \
    wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar && \
    wget https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.9/slf4j-simple-2.0.9.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.15.3/jackson-databind-2.15.3.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.15.3/jackson-core-2.15.3.jar && \
    wget https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.15.3/jackson-annotations-2.15.3.jar

# Copy project files
COPY pyproject.toml poetry.lock* README.md ./ 
COPY src/ ./src/
COPY config/ ./config/


# Install Python dependencies
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

# Create directories 
RUN mkdir -p /app/data/offsets /app/data/history /app/logs

# Run app
CMD ["python", "-m", "db_sync"]