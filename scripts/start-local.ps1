param(
  [int]$Port = 8765,
  [ValidateSet("auto", "mock")]
  [string]$Mode = "auto"
)

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

python .\backend\podmind.py --host 127.0.0.1 --port $Port --mode $Mode
