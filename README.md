# python-embedded-debezium-engine

> **A flexible, multi-database Change Data Capture (CDC) engine implemented in Python using the Debezium Embedded Connector via JPype. Designed for real-time streaming and synchronization between various SQL and NoSQL sources and targets.**

## 🌟 Overview

This project provides a robust, Python-native solution for real-time Change Data Capture (CDC). By embedding the Java-based Debezium connector via the JPype bridge, the system captures changes from a source database (e.g., PostgreSQL, MySQL) and processes them immediately within a Python environment.

The architecture is designed for high-fidelity data capture, supporting:
1.  **Real-Time Audit Logging:** Capturing every change event chronologically for historical analysis.
2.  **Batch Synchronization:** Processing events periodically for fault-tolerant data warehousing or system consolidation.

## 🚀 Getting Started

These instructions will get your project running for development and testing using Docker Compose.

### Prerequisites

* Docker and Docker Compose
* Python 3.10+
* Poetry (for dependency management)

### Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/shahnoor77/python-embedded-debezium-engine.git](https://github.com/shahnoor77/python-embedded-debezium-engine.git)
    cd python-embedded-debezium-engine
    ```

2.  **Download Debezium JARs:**
    Run the setup script to fetch the necessary Java libraries and connectors:
    ```bash
    # Ensure this script is executable (you may need to install sh/bash on Windows)
    sh ./scripts/download_jars.sh
    ```
    *This downloads all required JAR files into the `debezium/lib` directory.*

3.  **Configure Environment:**
    Copy the example environment file and fill in your database credentials:
    ```bash
    cp .env-exaample .env
    # Edit the .env file with your specific Postgres and MySQL credentials.
    ```

4.  **Start the Services:**
    Use Docker Compose to launch the PostgreSQL source database, the MySQL target database, and the Python application (`db-sync-app`).
    ```bash
    docker compose up --build -d
    # Monitor the application logs to confirm streaming starts:
    # docker logs -f db-sync-app
    ```

## ⚙️ Configuration

The engine properties are configured primarily via the `config/config.yaml` file. This includes both application settings and the required Java properties for the Debezium connector.

| Core Property | Purpose |
| :--- | :--- |
| `connector.class` | Specifies the source connector (e.g., `io.debezium.connector.postgresql.PostgresConnector`). |
| `database.hostname` | Hostname of the source database. |
| `database.server.name` | Unique logical name for the server (used for tracking offsets). |
| `database.user` | The database role authorized for replication. |
| `database.history.file.filename` | Path to the schema history file in the `data/history` directory. |

## 🏗️ Project Structure

| Directory | Purpose |
| :--- | :--- |
| `config/` | Stores application and Debezium configuration files. |
| `data/` | Persistent storage for Debezium offset and schema history files. |
| `debezium/lib/` | Location for all required Debezium and connector JAR files. |
| `docker/` | Docker Compose files, Dockerfile, and database initialization scripts. |
| `src/db_sync/core/` | Engine initialization, configuration, and the core JPype bridge logic (`debezium_engine.py`). |
| `src/db_sync/handlers/` | Logic for handling specific CDC operations (e.g., initial loading, schema changes, event processing). |
| `src/db_sync/connectors/` | Abstractions for connecting to different source/target databases. |
| `tests/` | Unit and integration tests for all Python components. |

## 💡 Key Implementation Details

The core CDC functionality is achieved through a stable **JPype bridge** implemented in `src/db_sync/core/debezium_engine.py`:

* **Cross-Language Consumer:** A Python class implements the required Java `ChangeConsumer` interface using `@JImplements`.
* **Asynchronous Queue:** The Java engine pushes the raw JSON event payload into a Python `Queue`. A separate Python thread processes this queue, ensuring the high-performance Java stream is never blocked by Python processing time.
* **Robust Data Handling:** The Python processing thread safely deserializes the raw JSON string payload, providing a clean Python dictionary to the user-defined synchronization logic.

## 🤝 Contribution

I welcome contributions to expand connector support, improve performance, or add robust error handling and new target consumers. Please open an issue or submit a pull request!

## 📜 License

This project is licensed under the **MIT License**.

See the `LICENSE` file for details.