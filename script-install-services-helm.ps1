helm upgrade --install bitnami-kafka-release ./helm-chart/bitnami-kafka-chart --values ./helm-chart/bitnami-kafka-chart/values-miniset.yaml
helm upgrade --install grafana-release ./helm-chart/grafana-chart --values ./helm-chart/grafana-chart/values.yaml --values ./helm-chart/grafana-chart/values-secret.yaml
helm upgrade --install influxdb-release ./helm-chart/influxdb-chart --values ./helm-chart/influxdb-chart/values.yaml --values ./helm-chart/influxdb-chart/values-secret.yaml
helm upgrade --install kafka-ui-release ./helm-chart/kafka-ui-chart --values ./helm-chart/kafka-ui-chart/values.yaml
helm upgrade --install postgres-release ./helm-chart/postgres-chart --values ./helm-chart/postgres-chart/values.yaml --values ./helm-chart/postgres-chart/values-secret.yaml
helm upgrade --install superset-release ./helm-chart/superset-chart --values ./helm-chart/superset-chart/values.yaml --values ./helm-chart/superset-chart/values-secret.yaml
