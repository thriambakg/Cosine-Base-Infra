# Update legislators-current.csv from the congress-legislators GitHub gh-pages branch
# Run this script periodically to keep the CSV file up to date
# 
# According to the congress-legislators README, CSV files are provided directly
# from the gh-pages branch: https://unitedstates.github.io/congress-legislators/legislators-current.csv

param(
    [string]$OutputPath = "$PSScriptRoot\..\static-files\lists\congress-legislators.csv",
    [string]$CsvUrl = "https://unitedstates.github.io/congress-legislators/legislators-current.csv"
)

Write-Host "Updating legislators CSV from congress-legislators gh-pages branch..." -ForegroundColor Cyan

# Ensure output directory exists
$outputDir = Split-Path $OutputPath -Parent
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

# Download the CSV file directly from gh-pages
Write-Host "Downloading CSV from: $CsvUrl" -ForegroundColor Cyan

try {
    $response = Invoke-WebRequest -Uri $CsvUrl -UseBasicParsing -ErrorAction Stop
    
    # Write to file
    $response.Content | Out-File -FilePath $OutputPath -Encoding utf8 -NoNewline
    
    # Verify file was created and has content
    if (-not (Test-Path $OutputPath)) {
        Write-Host "Error: Output file was not created" -ForegroundColor Red
        exit 1
    }
    
    $fileContent = Get-Content $OutputPath -Raw
    if ([string]::IsNullOrWhiteSpace($fileContent)) {
        Write-Host "Error: Downloaded file is empty." -ForegroundColor Red
        exit 1
    }
    
    $lineCount = (Get-Content $OutputPath | Measure-Object -Line).Lines
    Write-Host "Successfully downloaded legislators CSV!" -ForegroundColor Green
    Write-Host "Lines: $lineCount" -ForegroundColor Gray
    
} catch {
    Write-Host "Error downloading CSV file: $_" -ForegroundColor Red
    Write-Host "URL: $CsvUrl" -ForegroundColor Yellow
    exit 1
}

Write-Host "Output: $OutputPath" -ForegroundColor Gray
Write-Host ""
Write-Host "Next step: Run terraform apply to upload the updated file to S3" -ForegroundColor Cyan
