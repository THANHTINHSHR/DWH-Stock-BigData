# DWH-Stock-BigData: Real-time Stock Data Warehouse System 3.0

DWH-Stock-BigData v3 is a production-grade, Kubernetes-native real-time stock data warehouse. Uses Helm-based modular deployment and simulates a product-ready architecture.

## 📚 Table of Contents
- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Features](#features)
- [System Architecture](#system-architecture)
- [Informer-AI Module](#informer-ai-module)
  - [Functionality](#functionality)
  - [Informer-AI-Architecture](#informer-ai-architecture)
- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Docker Setup](#docker-setup)
  - [Kubernetes Setup](#kubernetes-setup)
- [Screenshots](#screenshots)
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
### 🤖 Informer-AI Module

#### Functionality

- Input data is loaded from **S3** (time-series format).
- Performs **short-term forecasting** using the **Informer** model.
- Supports **automatic training and prediction** workflows.
- Automatically generates **charts and dashboards** on:
  - **Grafana**
          ![Grafana-informer](images/grafana_ticker_predict_1.png)
  - **Superset**
          ![Superset-informer](images/superset_ticker_predict.png)
- Currently supports only the **`ticker`** data stream.
- Codebase is **modular** and **easily extendable** for other data flows.
### Informer-AI-Architecture
Informer-AI system architecture overview::
![Informer-AI-Architecture](images/System_Architecture_Informer.png)

## 🔧 Installation
### 📥 Prerequisites
- [Git](https://git-scm.com/downloads)
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Kubernetes](https://kubernetes.io/docs/tasks/tools/install-kubectl-windows)
- [Minikube](https://minikube.sigs.k8s.io/)
- [Helm](https://helm.sh/)
-   **Clone Repository & Download Dependencies:**
    ```bash
    git clone -b Airflow-Orchestration https://github.com/THANHTINHSHR/DWH-Stock-BigData.git

    cd DWH-Stock-BigData
    ```
    - Download the required `jars` folders from this [jars](https://drive.google.com/file/d/19l1vo4G3sWoPF1UYXeGnaES5Ny_m9Xm8/view?usp=sharing). Extract and copy all file `.jar` into folder `/jars`
    - Download [spark-3.5.6-bin-hadoop3.tgz](https://dlcdn.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz) and copy to folder `/tar`.
    - Download [wheels](https://drive.google.com/file/d/19l1vo4G3sWoPF1UYXeGnaES5Ny_m9Xm8/view?usp=drive_link). Extract and copy file `.whl` into folder `/wheels`

### 🐳 Docker Setup
 📌 **Note: Project runs in isolated Docker containers without orchestration support.**

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
    -  Update your .env file with the `INFLUXDB_TOKEN` and `GRAFANA_KEY` you obtained in the previous step.

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
        - informerAI_predictor
        ```bash
        docker-compose -f docker-compose-predictor.yml build
        ```
### 🌐 Kubernetes Setup

1. **Download/Build Images**
   - Build Docker images locally using the provided PowerShell script:
     ```powershell
     ./script-build-images.ps1
     ```

2. **Set Up Secret Variables**
   - Fill in environment variables in each file under `secret-tmp/`.
   - Then copy the generated `values-secret.yaml` files into the corresponding folders under `helm-chart/`.(Except `project-env-chart`)
    ![Secret tmp](images/secret-tmp.png)

3. **Install Services with Helm**
   - Use the PowerShell script to install all services via Helm:
     ```powershell
     ./script-install-services-helm.ps1
     ```
    - Access service:
        + Access Influxdb at [http://localhost:8086](http://localhost:8086) (login and save key):
        ```
        kubectl port-forward svc/influxdb-release-influxdb-chart 8086:8086
        ```
        + Access Grafana at [http://localhost:3000](http://localhost:3000) (login and save key):
        ```
        kubectl port-forward svc/grafana-release-grafana-chart 3000:3000
        ```
        + Access Kafka UI at [http://localhost:9090](http://localhost:9090):
        ```
        kubectl port-forward svc/kafka-ui-release-kafka-ui-chart 9090:9090
        ```
        + Access Superset at [http://localhost:8088](http://localhost:8088) (login and save key):
        ```
        kubectl port-forward svc/superset-release-superset-chart 8088:8088
    - Update Service key:
        + Open file `helm-chart\secret-tmp\project-env-chart\values-secret.yaml`, update your usernames, passwords, keys, ... .Copy this file and add it to `helm-chart\project-env-chart`.
        + Run script to install main project:
        ```
        ./script-install-project-helm.ps1
        ```
    - Access Airflow UI at [http://localhost:8080/](http://localhost:8080/):
       ```
       kubectl port-forward svc/airflow-release-api-server 8080:8080
        ```
        - Airflow Dags
        ![Airflow UI](images/Airflow-2.png)
        - Airflow Ticker Dag
        ![Airflow UI](images/Airflow-3.png)

## 📸 Screenshots
- InfluxDB:
    ![InfluxDB](images/influx_Interface.png)
- Grafana:
    - Grafana-Trade
    ![Grafana-trade](images/grafana_trade.png)
    - Grafana-Ticker
    ![Grafana-ticker](images/grafana_ticker.png)
    - Grafana-Bookticker
    ![Grafana-bookticker](images/grafana_bookticker.png)

- Superset:
    ![Superset-dashboard](images/superset_dashboard.png)

🎯 Key Takeaways & Experiences
- Gained practical experience in designing and operating a real-time data warehouse system.

- Integrated end-to-end workflows, from data ingestion to processing, storage, and visualization.

- Strengthened skills in deploying microservices using Kubernetes and Helm.

- Improved understanding of big data streaming with Kafka and batch processing using Spark.

- Applied modular and scalable software design for better maintainability and extensibility.

- Gained experience in Airflow orchestration, including DAG scheduling, dependency control, and automated pipeline triggering.

- Documented technical decisions, challenges, and lessons learned throughout development.

- 📁 Experience logs are available in the notebooks/ directory.
📌 Note: These logs are primarily written in Vietnamese.
## Contact

If you have any questions, suggestions, or would like to discuss this project further, feel free to reach out:

- **Email**: thanhtinh.de@gmail.com
- **Phone**: 0899986747
- **ZALO**: 0356657722