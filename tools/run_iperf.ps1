Param(
  [Parameter(Mandatory=$true)][ValidateSet('server','client')] [string]$mode,
  [string]$server,
  [int]$duration = 30
)
if ($mode -eq 'server') {
  Write-Host 'Starting iperf3 server on port 5201'
  iperf3 -s
} else {
  if (-not $server) { Write-Host 'Provide server IP'; exit 1 }
  Write-Host "Running iperf3 client to $server for $duration seconds"
  iperf3 -c $server -t $duration -P 4
}
