param(
  [string]$ImageName = "podmind:latest"
)

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

docker build -t $ImageName .
minikube image load $ImageName
kubectl apply -f .\demo\university-workloads.yaml
kubectl apply -f .\deploy\podmind.yaml
kubectl -n podmind rollout status deployment/podmind

Write-Host "Dashboard:"
Write-Host "  minikube service podmind -n podmind --url"
Write-Host "  or open http://localhost:8765 after: kubectl -n podmind port-forward svc/podmind 8765:8765"
