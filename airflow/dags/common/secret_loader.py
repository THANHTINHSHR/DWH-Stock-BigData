def load_secrets():
    from airflow.providers.cncf.kubernetes.secret import Secret  # type: ignore

    secret_keys = [
        "AI_SPARK_MODE", "AI_SPARK_LOCAL_DIR", "AI_SPARK_APP_NAME",
        "SPARK_LOCAL_DIR", "SPARK_MODE", "WSS_ENDPOINT", "URL_TOP", "LIMIT",
        "STREAM_TYPES", "AWS_DEFAULT_REGION", "BUCKET_NAME", "ROOT_DB", "ATHENA_DB",
        "S3_STAGING_DIR", "BOOTSTRAP_SERVERS", "BINANCE_TOPIC", "NUM_PARTITIONS",
        "GRAFANA_URL", "GRAFANA_DB_URL", "INFLUXDB_URL", "INFLUXDB_ORG", "INFLUXDB_BUCKET",
        "SUPERSET_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "GRAFANA_KEY",
        "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD", "INFLUXDB_TOKEN",
        "SUPERSET_USERNAME", "SUPERSET_PASSWORD", "SUPERSET_SECRET_KEY",
        "AI_APP_NAME", "REPARTITION", "TRAIN_RATIO", "VAL_RATIO", "BATCH_SIZE",
        "N_DAYS_AGO", "MAX_DIRECTORIES", "SEQUENCE_LENGTH", "PREDICTION_LENGTH", "NUM_EPOCHS"
    ]

    return [Secret("env", key, secret="project-secret", key=key) for key in secret_keys]
