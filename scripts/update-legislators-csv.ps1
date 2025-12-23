# Update legislators-current.csv from the congress-legislators GitHub gh-pages branch
# Run this script periodically to keep the CSV file up to date
# 
# According to the congress-legislators README, CSV files are provided directly
# from the gh-pages branch: https://unitedstates.github.io/congress-legislators/legislators-current.csv
#
# This script merges new rows from the downloaded CSV into the existing CSV file
# instead of replacing it entirely, using bioguide_id as the unique identifier.

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

# Read existing CSV if it exists
$existingRows = @()
$existingBioguideIds = @{}
$existingLineCount = 0

if (Test-Path $OutputPath) {
    Write-Host "Reading existing CSV file..." -ForegroundColor Cyan
    try {
        $existingRows = Import-Csv -Path $OutputPath -Encoding UTF8
        $existingLineCount = $existingRows.Count
        Write-Host "Found $existingLineCount existing rows" -ForegroundColor Gray
        
        # Create a dictionary of existing bioguide_ids for quick lookup
        foreach ($row in $existingRows) {
            if ($row.bioguide_id) {
                $existingBioguideIds[$row.bioguide_id] = $true
            }
        }
        Write-Host "Found $($existingBioguideIds.Count) unique bioguide_ids in existing file" -ForegroundColor Gray
    } catch {
        Write-Host "Warning: Could not read existing CSV file. Will create new file. Error: $_" -ForegroundColor Yellow
        $existingRows = @()
        $existingBioguideIds = @{}
    }
} else {
    Write-Host "No existing CSV file found. Will create new file." -ForegroundColor Yellow
}

# Download the new CSV file
Write-Host "Downloading CSV from: $CsvUrl" -ForegroundColor Cyan

try {
    $response = Invoke-WebRequest -Uri $CsvUrl -UseBasicParsing -ErrorAction Stop
    
    # Save to temporary file first
    $tempFile = [System.IO.Path]::GetTempFileName()
    $response.Content | Out-File -FilePath $tempFile -Encoding utf8 -NoNewline
    
    # Verify temp file has content
    $tempContent = Get-Content $tempFile -Raw
    if ([string]::IsNullOrWhiteSpace($tempContent)) {
        Write-Host "Error: Downloaded file is empty." -ForegroundColor Red
        Remove-Item $tempFile -ErrorAction SilentlyContinue
        exit 1
    }
    
    # Parse the new CSV
    $newRows = Import-Csv -Path $tempFile -Encoding UTF8
    $newLineCount = $newRows.Count
    Write-Host "Downloaded $newLineCount rows from source" -ForegroundColor Gray
    
    # Identify new rows (rows with bioguide_ids not in existing file)
    $newRowsToAdd = @()
    foreach ($row in $newRows) {
        if ($row.bioguide_id -and -not $existingBioguideIds.ContainsKey($row.bioguide_id)) {
            $newRowsToAdd += $row
        }
    }
    
    $newCount = $newRowsToAdd.Count
    Write-Host "Found $newCount new rows to add" -ForegroundColor Cyan
    
    if ($newCount -eq 0) {
        Write-Host "No new rows to add. Existing file is up to date." -ForegroundColor Green
        Remove-Item $tempFile -ErrorAction SilentlyContinue
    } else {
        # Merge: existing rows + new rows
        $mergedRows = $existingRows + $newRowsToAdd
        $mergedCount = $mergedRows.Count
        
        # Write merged CSV
        $mergedRows | Export-Csv -Path $OutputPath -Encoding UTF8 -NoTypeInformation
        
        Write-Host "Successfully merged CSV file!" -ForegroundColor Green
        Write-Host "  Existing rows: $existingLineCount" -ForegroundColor Gray
        Write-Host "  New rows added: $newCount" -ForegroundColor Gray
        Write-Host "  Total rows: $mergedCount" -ForegroundColor Gray
        
        Remove-Item $tempFile -ErrorAction SilentlyContinue
    }
    
} catch {
    Write-Host "Error downloading or processing CSV file: $_" -ForegroundColor Red
    Write-Host "URL: $CsvUrl" -ForegroundColor Yellow
    exit 1
}

Write-Host "Output: $OutputPath" -ForegroundColor Gray
Write-Host ""
Write-Host "Next step: Run terraform apply to upload the updated file to S3" -ForegroundColor Cyan
