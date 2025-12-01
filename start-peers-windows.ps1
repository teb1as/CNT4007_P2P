# start-peers-windows.ps1
# Opens a new PowerShell window for each peer and runs: java peerProcess <peerID>
# Usage: edit the $peers array below if you want different peer IDs.

$root = "C:\coding shit\CNTPROJ\CNT4007_P2P"
# default peers (adjust as needed)
$peers = @(1001,1002,1003,1004,1005,1006)

foreach ($id in $peers) {
    # Build the command to run inside the new window. Use Set-Location so cd works with spaces.
    $command = "Set-Location -LiteralPath '$root'; Write-Host 'Starting peer $id (stdout follows)'; java peerProcess $id"

    Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit","-Command",$command
    Start-Sleep -Milliseconds 400
}

Write-Host "Launched $($peers.Count) peer windows. Close windows when finished or press Ctrl+C here."