docker build -t bitnami-kafka-custom:4.0.0-debian-12-r8 -f docker-file/Dockerfile.bitnami-kafka .
docker build -t kafka-ui-custom:v0.7.2 -f docker-file/Dockerfile.kafka-ui .
docker build -t grafana-custom:10.4.2 -f docker-file/Dockerfile.grafana .
docker build -t influxdb-custom:2.7 -f docker-file/Dockerfile.influxdb .
docker build -t informer-ai:2.0 -f docker-file/Dockerfile.informer .
docker build -t postgres-custom:17.5.0 -f docker-file/Dockerfile.postgres .
docker build -t superset-custom:5.0.0 -f docker-file/Dockerfile.superset .

docker build -t dwh-stock-bigdata:3.0 -f docker-file/Dockerfile.project .
docker build -t informer-ai:3.0 -f docker-file/Dockerfile.informer .

docker build -t airflow-custom:3.0.2 -f docker-file/Dockerfile.airflow .
