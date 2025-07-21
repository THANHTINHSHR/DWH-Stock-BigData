Write-Host "🔧 Building Docker images..."

$images = @(
  "bitnami-kafka-custom:4.0.0-debian-12-r8|Dockerfile.bitnami-kafka",
  "grafana-custom:10.4.2|Dockerfile.grafana",
  "influxdb-custom:2.7|Dockerfile.influxdb",
  "informer-ai:2.0|Dockerfile.informer",
  "kafka-ui-custom:v0.7.2|Dockerfile.kafka-ui",
  "postgres-custom:17.5.0|Dockerfile.postgres",
  "dwh-stock-bigdata:2.0|Dockerfile.project",
  "superset-custom:5.0.0|Dockerfile.superset"
)

foreach ($entry in $images) {
  $parts = $entry -split "\|"
  $tag = $parts[0]
  $file = $parts[1]
  Write-Host "`n📦 Building $tag from $file..."
  docker build -t $tag -f "docker-file/$file" . || {
    Write-Error "❌ Failed to build $tag"
    exit 1
  }
}

Write-Host "`n✅ All images built successfully."
