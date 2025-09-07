# Test script to verify layer structure
# This script helps debug layer issues by checking the structure and contents

Write-Host "LAMBDA LAYER TEST SCRIPT" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Green

# Check if python directory exists
if (Test-Path "python") {
    Write-Host "SUCCESS: python directory exists" -ForegroundColor Green
    
    # List contents of python directory
    Write-Host "Contents of python directory:" -ForegroundColor Cyan
    Get-ChildItem -Path "python" | Select-Object Name, Mode, Length | Format-Table -AutoSize
    
    # Check for specific packages
    $packages = @("requests", "numpy", "pandas", "yfinance", "dotenv")
    Write-Host "Checking for required packages:" -ForegroundColor Cyan
    foreach ($package in $packages) {
        if (Test-Path "python\$package") {
            Write-Host "  SUCCESS: $package found" -ForegroundColor Green
        } else {
            Write-Host "  ERROR: $package NOT found" -ForegroundColor Red
        }
    }
    
    # Check for site-packages structure
    if (Test-Path "python\lib\python3.11\site-packages") {
        Write-Host "SUCCESS: site-packages directory exists" -ForegroundColor Green
        Write-Host "Contents of site-packages:" -ForegroundColor Cyan
        Get-ChildItem -Path "python\lib\python3.11\site-packages" | Select-Object -First 10 Name | Format-Table -AutoSize
    } else {
        Write-Host "ERROR: site-packages directory NOT found" -ForegroundColor Red
    }
    
} else {
    Write-Host "ERROR: python directory does not exist" -ForegroundColor Red
    Write-Host "Run the build script first!" -ForegroundColor Yellow
    exit 1
}

# Check if layer.zip exists
if (Test-Path "layer.zip") {
    Write-Host "SUCCESS: layer.zip exists" -ForegroundColor Green
    
    # Get layer size
    $layerSize = (Get-Item "layer.zip").Length
    $layerSizeMB = [math]::Round($layerSize / 1MB, 2)
    Write-Host "Layer size: $layerSizeMB MB" -ForegroundColor Cyan
    
    # Check layer contents
    Write-Host "Contents of layer.zip:" -ForegroundColor Cyan
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip = [System.IO.Compression.ZipFile]::OpenRead("$PWD\layer.zip")
    $entries = $zip.Entries | Select-Object -First 20
    Write-Host "First 20 entries in layer:"
    $entries | ForEach-Object { Write-Host "  $($_.FullName)" }
    $zip.Dispose()
    
    # Check if requests is in the layer
    $zip = [System.IO.Compression.ZipFile]::OpenRead("$PWD\layer.zip")
    $requestsEntries = $zip.Entries | Where-Object { $_.FullName -like "*requests*" }
    if ($requestsEntries.Count -gt 0) {
        Write-Host "SUCCESS: requests module found in layer.zip" -ForegroundColor Green
        Write-Host "  Found $($requestsEntries.Count) requests-related entries"
    } else {
        Write-Host "ERROR: requests module NOT found in layer.zip" -ForegroundColor Red
    }
    $zip.Dispose()
    
} else {
    Write-Host "ERROR: layer.zip does not exist" -ForegroundColor Red
    Write-Host "Run the build script first!" -ForegroundColor Yellow
}

Write-Host "================================" -ForegroundColor Green
Write-Host "LAMBDA LAYER TEST COMPLETE" -ForegroundColor Green
