# DWH-Stock-BigData: Real-time Stock Data Warehouse System 3.0

DWH-Stock-BigData v3 is a production-grade, Kubernetes-native real-time stock data warehouse. Uses Helm-based modular deployment and simulates a product-ready architecture.

## 📚 Table of Contents
- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Changes Compared to Previous Version](#changes-compared-to-previous-version)

- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Docker Setup](#docker-setup)
  - [Kubernetes Setup](#kubernetes-setup)
- [Running the Application](#running-the-application)
  - [Run On Docker](#run-on-docker)
  - [Run On Kubernetes](#run-on-kubernetes)
    - [Starting Minikube](#starting-minikube)
    - [Access Web UIs](#access-web-uis)
- [Informer-AI Module](#informer-ai-module)
  - [Functionality](#functionality)
  - [Informer-AI-Architecture](#informer-ai-architecture)
  - [How to Run](#how-to-run-informer)


- [Demo](#demo)
- [Troubleshooting](#troubleshooting)
- [Key Takeaways & Experiences](#key-takeaways--experiences)
- [Contact](#contact)

## 📌 Overview

- **Project**: DWH-Stock-BigData v3
- **Purpose**: Real-time stock data warehouse system, production-ready
- **Architecture**: Modular microservices using containerization
- **Orchestration**: Managed with **Apache Airflow**
- **Deployment**: Supports both **Docker** and **Kubernetes (via Helm)**
- **AI Module**: Includes optional **Informer-AI** for short-term time-series prediction
### 🛠️ Technologies Used

- **Kafka** (Bitnami, KRaft): Stream ingestion from Binance.
- **Spark Structured Streaming**: Real-time & batch processing.
- **InfluxDB**: Time-series storage for real-time data.
- **Amazon S3**: Batch data storage.
- **Athena**: SQL queries over S3.
- **Superset**: BI dashboard from Athena.
- **Grafana**: Real-time visualization from InfluxDB.
- **Informer-AI (optional)**: Short-term time-series prediction.
- **Airflow**: Workflow orchestration.
- **Docker & Helm**: Build and deploy microservices.
- **Kubernetes (Minikube)**: Container orchestration platform.
## 🚀 Features

- **Production-Oriented**: Configured for reproducibility and infrastructure-as-code.
- **Modular & Scalable**: Designed with OOP principles for easy extension and future module upgrades.
- **Real-Time Processing**: Supports high-throughput streaming with Kafka and Spark.
- **Automated Visualization**: Uses Superset for dashboard auto-generation.
- **Flexible Deployment**: Runs on both Docker and Kubernetes via Helm.
- **Pipeline Orchestration**: Managed and triggered using Airflow.
- **AI Module**: Includes optional **Informer-AI** for short-term time-series prediction.
## 🧱 System Architecture
Below is a high-level overview of the system architecture:
![System Architecture](images/System_Architecture_3.png)
## 🔧 Installation
### 📥 Prerequisites
- [Git](https://git-scm.com/downloads)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows)
- [Minikube](https://minikube.sigs.k8s.io/)
- [Helm](https://helm.sh/)
-   **Clone Repository & Download Dependencies:**
    ```bash
    git clone https://github.com/THANHTINHSHR/DWH-Stock-BigData/tree/Airflow-Orchestration
    cd DWH-Stock-BigData
    ```
    - Download the required `jars`folders from this [Google Drive link.](https://drive.google.com/file/d/19l1vo4G3sWoPF1UYXeGnaES5Ny_m9Xm8/view?usp=sharing). Extract and copy all file `.jar` into folder `/jars`
    - Download [spark-3.5.6-bin-hadoop3.tgz](https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz) and copy to folder `/tar`.
### 🐳 Docker Setup
 📌 **Note: In Docker, the project runs as isolated containers and does not support an orchestrator.**

1.  **Configure environment variables:**
    - Create a `.env` file in the project root (you can copy `.env.example` if provided in the repository) and populate it with your specific configuration values.
    Pay close attention to API keys, tokens, and AWS credentials. Some keys (like for InfluxDB and Grafana) will be obtained in the next step.
    For `SUPERSET_SECRET_KEY`, generate a strong random string (e.g., using `openssl rand -base64 32`) and add it to your `.env` file now.
    ![env example](images/env_example.png)
2.  **Initial Service Startup & Key Generation:**

    -   Run:
        ```bash
        docker-compose -f docker-compose-services.yml up --build
        ```
    - Access the InfluxDB UI (e.g., `http://localhost:8086`) and Grafana UI (e.g., `http://localhost:3000`) to complete their initial setup and generate the necessary API tokens/keys (`INFLUXDB_TOKEN`, `GRAFANA_KEY`).


3.  **Configure Keys & Finalize Setup:**
    -   Update your `.env` file with the `INFLUXDB_TOKEN` and `GRAFANA_KEY` obtained in the previous step.
    -   Ensure the `SUPERSET_SECRET_KEY` (generated in step 2) is also correctly set in your `.env` file.
    -   After config, build two container:

        - dwh_stock_bigdata
        ```bash
        docker-compose -f docker-compose-project.yml build
        ```
    - For Informer module:
        - informerAI_trainer
        ```bash
        docker-compose -f docker-compose-trainer.yml build
        ```
        - informerAI_trainer
        ```bash
        docker-compose -f docker-compose-predictor.yml build
        ```
        All components should now be fully functional with the correct API keys. After this step, your system is ready to operate as described in the "Running the Application" section.
### 🌐 Kubernetes Setup

1.  **Dowload/Build Images:**
    - Build images locally using the provided PowerShell script:
        ```
        ./script-build-images.ps1
        ```
2.  **Install Services By Helm:**
3.  **Install Services By Helm:**
    - Install services using Helm by PowerShell script:
        ```
        ./script-install-services-helm.ps1
        ```