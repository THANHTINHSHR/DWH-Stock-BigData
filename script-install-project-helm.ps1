helm upgrade --install project-env ./helm-chart/project-env-chart --values ./helm-chart/project-env-chart/values-secret.yaml
helm upgrade --install project-env ./helm-chart/project-env-chart --values ./helm-chart/project-env-chart/values-secret.yaml
helm upgrade --install airflow-release ./helm-chart/airflow 
