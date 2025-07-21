# DWH-Stock-BigData: Real-time Stock Data Warehouse System 2.0
> 📌 Note: For the complete system architecture and detailed project description, please refer to the original README at:  
> https://github.com/THANHTINHSHR/DWH-Stock-BigData/blob/master/README.md

This branch introduces major improvements in both infrastructure and data processing pipeline.

## 📚 Table of Contents

- [Overview](#overview)
- [Changes Compared to Previous Version](#changes-compared-to-previous-version)
- [Architecture](#architecture)
- [Knowledge Improvements](#knowledge-improvements)
- [How to Run](#how-to-run)
- [Screenshots / Demo](#screenshots--demo)
- [Reference to Old Version](#reference-to-old-version)

## 📌 Overview
- Migrated the project from a local monolithic setup (v1) to a Kubernetes-based microservices architecture.
- Containerized all components and deployed them using Docker & Helm on Minikube.
- Refactored system into modular services for better flexibility and scalability.
- Designed individual Helm charts for each service to streamline deployment and maintenance.
## 🔁 Changes Compared to Previous Version

- **Kafka**  
  Switched from `cp-kafka` to `bitnami/kafka:4.0.0-debian-12-r8`  
  ➤ Now using **KRaft** mode for native consensus

- **Grafana**  
  Locked version to `grafana:10.4.2`

- **InfluxDB**  
  Using `influxdb:2.7.0`

- **Kafka UI**  
  Using `provectuslabs/kafka-ui:v0.7.2`

- **Superset**  
  Upgraded to `apache/superset:5.0.0`

- **Spark**  
  Upgraded to `spark-3.5.6-bin-hadoop3`

- **Custom Applications**  
  Updated image tags:  
  ➤ `dwh-stock-bigdata:2.0`  
  ➤ `informer-ai:2.0`

- **Project Structure**  
  ➤ Renamed folder `docker/` → `docker-file/`  
  ➤ Docker Compose now builds from Dockerfiles instead of pulling prebuilt images
- **Spark Session Initialization**  
  ➤ Refactored logic for creating SparkSession  
  ➤ Now supports consistent configuration across **local**, **Docker**, and **Kubernetes** environments  
  ➤ Dynamically handles environment variables and context detection to reduce duplication and improve portabilit
- **Deployment**  
  ➤ Introduced `helm-chart/` folder with custom Helm charts per service  
  ➤ Ensured all services run in the same Kubernetes namespace  
  ➤ Secured sensitive environment variables using `values-secret.yaml` and `secret.yaml` (Kubernetes best practices)

## 🏗️ Architecture

The overall system architecture remains consistent with the previous version.  
For full details, refer to the original [README.md](https://github.com/THANHTINHSHR/DWH-Stock-BigData/blob/master/README.md).


## 🧠 Knowledge Improvements

- **Apache Kafka**: Deepened understanding of Kafka architecture, topics, and partitioning.
- **cp-Kafka vs Bitnami Kafka**: Migrated from cp-Kafka to Bitnami Kafka image, explored differences and configurations.
- **Kafka KRaft Mode**: Switched from ZooKeeper to KRaft mode, enhancing knowledge about KRaft-based cluster management.
- **Minikube**: Set up and managed a local Kubernetes cluster using Minikube for development and testing.
- **Kubernetes (K8s)**: Gained strong understanding of container orchestration, service discovery, secret and config management, resource isolation using namespaces, and inter-service communication.
- **Helm Charts**:
  - Understand core Kubernetes objects used in Helm: `Deployment`, `Service`, `Ingress`, `Job`, and `PersistentVolumeClaim`.
  - Capable of writing custom Helm charts for each microservice to enable scalable and modular deployment.
  - Understand clearly how Pods, Services, and PVCs interact in a Kubernetes cluster.
  - Confident in handling secure deployment by managing sensitive variables via `values-secret.yaml` and `Secret` resources.
  - Apply proper Helm structuring to separate templates, values, and environment-specific configs.
  - Ensure services communicate smoothly within the same namespace.
  - Familiar with Helm release lifecycles: install, upgrade, rollback, and delete.

## 🚀 How to Run

### 🧰 Prerequisites

Make sure the following tools are installed:

- [Minikube](https://minikube.sigs.k8s.io/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/)

#### Quick install on Windows (via winget):

```bash
winget install Kubernetes.minikube
winget install Kubernetes.kubectl
winget install Helm.Helm
```
### RUN:
- Start minikube with drive docker: 
```bash
minikube start --driver=docker
& minikube -p minikube docker-env --shell powershell | Invoke-Expression

```
- Get minikube ip:
  ```bash
  minikube ip
  ```
  + Copy this ip to `helm-chart\kafka-ui-chart\values.yaml`
- Create values-secret.yaml:
  + Access to `helm-chart\secret-tmp`, copy, rename to *-chart\values-secret.yaml, change secret values to your values ( ex username, password, ... token add later)


- Build images:
```bash
./build-images.ps1
```
- Run services:
```bash
./run-services.ps1
```
- Get Token Key:
  + Grafna : 
    - Open port-forward
    ```bash
    kubectl port-forward svc/grafana-release-grafana-chart 3000:3000

    ```
    - Access at `http://localhost:3000` , 
    - Log in using `grafana_admin_user` and `grafana_admin_password` from `helm-chart\grafana-chart\values-secret.yaml` file. Create service account, add service account token, save it in `helm-chart\grafana-chart\values-secret.yaml` and `helm-chart\informer-chart\values-secret.yaml`
  + InfluxDB:
    - Open port-forward
    ```bash
    kubectl port-forward svc/influxdb-release-influxdb-chart 8086:8086
    ```
    - Access at `http://localhost:8086` to interact directly with InfluxDB.
    - Get token, save to `helm-chart\grafana-chart\values-secret.yaml` and `helm-chart\informer-chart\values-secret.yaml`
  + Superset:
    - Open port-forward
    ```bash
    kubectl port-forward svc/superset-release-superset-chart 8088:8088
    ```
    - Access at `http://localhost:8088`. Log in using `admin_username` and `admin_password` in `helm-chart\superset-chart\values-secret.yaml` 
    - Get token, save to `helm-chart\grafana-chart\values-secret.yaml` and `helm-chart\informer-chart\values-secret.yaml`

- Run main project:
```bash
./run-project.ps1
```
- Run informer-ai (optional):
```bash
./run-informer.ps1
```

> 📌 **Note:**  
> You can also run the project using Docker instead of Minikube.  
> Ensure your `.env` file is correctly filled with required environment variables.



## 🖼️ Screenshots / Demo
GIF or image/video link showing it in action.

## 🔗 Reference to Old Version
For full project structure and previous setup, check:
[Old README](https://github.com/THANHTINHSHR/DWH-Stock-BigData/blob/master/README.md)
