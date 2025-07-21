
$charts = @(
  "informer-release|informer-chart|values.yaml,values-secret.yaml"
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
