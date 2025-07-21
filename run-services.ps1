$charts = @(
  "bitnami-kafka-release|bitnami-kafka-chart|values-miniset.yaml",
  "grafana-release|grafana-chart|values.yaml,values-secret.yaml",
  "influxdb-release|influxdb-chart|values.yaml,values-secret.yaml",
  "kafka-ui-release|kafka-ui-chart|values.yaml",
  "postgres-release|postgres-chart|values.yaml,values-secret.yaml",
  "superset-release|superset-chart|values.yaml,values-secret.yaml"
)

foreach ($entry in $charts) {
  $parts = $entry -split '\|'
  $release = $parts[0]
  $chart = $parts[1]
  $values = $parts[2].Split(',')

  $cmd = "helm upgrade --install $release ./helm-chart/$chart"
  foreach ($val in $values) {
    $cmd += " --values ./helm-chart/$chart/$val"
  }

  Write-Host "`n🚀 Deploying $release..."
  Invoke-Expression $cmd
}
