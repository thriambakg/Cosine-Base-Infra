# PowerShell build script for Lambda Layer with all dependencies
# This script creates a layer package with all the heavy dependencies

# Get parameters from environment variables or use defaults
$PythonCmd = $env:PYTHON_CMD
if (-not $PythonCmd) {
    $PythonCmd = "python3.11"
}

$RequirementsFile = $env:REQUIREMENTS_FILE
if (-not $RequirementsFile) {
    $RequirementsFile = "chat-agent-dependencies.txt"
}

Write-Host "Building Lambda Layer with dependencies..." -ForegroundColor Green
Write-Host "Using Python command: $PythonCmd" -ForegroundColor Cyan
Write-Host "Using requirements file: layer-definitions/$RequirementsFile" -ForegroundColor Cyan

# Clean up any existing build artifacts
if (Test-Path "python") {
    Remove-Item -Recurse -Force "python"
}
if (Test-Path "layer.zip") {
    Remove-Item -Force "layer.zip"
}

# Create python directory structure
New-Item -ItemType Directory -Path "python" -Force | Out-Null

# Install dependencies into the python directory
Write-Host "Installing dependencies using $PythonCmd..." -ForegroundColor Cyan
& $PythonCmd -m pip install -r "layer-definitions/$RequirementsFile" -t python/ --no-user

if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to install dependencies"
    exit 1
}

# Fix OpenTelemetry entry points issue
Write-Host "Fixing OpenTelemetry entry points..." -ForegroundColor Yellow
$entryPointsPath = "python\opentelemetry_api-1.36.0.dist-info\entry_points.txt"
if (Test-Path $entryPointsPath) {
    $entryPointsContent = @"
[opentelemetry_context]
contextvars_context = opentelemetry.context.contextvars_context:ContextVarsRuntimeContext
"@
    Set-Content -Path $entryPointsPath -Value $entryPointsContent -Encoding UTF8
    Write-Host "Updated OpenTelemetry entry points" -ForegroundColor Green
} else {
    Write-Host "OpenTelemetry entry points file not found, creating..." -ForegroundColor Yellow
    $opentelemetryDir = "python\opentelemetry_api-1.36.0.dist-info"
    if (Test-Path $opentelemetryDir) {
        New-Item -Path $entryPointsPath -ItemType File -Force | Out-Null
        $entryPointsContent = @"
[opentelemetry_context]
contextvars_context = opentelemetry.context.contextvars_context:ContextVarsRuntimeContext
"@
        Set-Content -Path $entryPointsPath -Value $entryPointsContent -Encoding UTF8
        Write-Host "Created OpenTelemetry entry points" -ForegroundColor Green
    }
}

# Remove unnecessary files to reduce size
Write-Host "Cleaning up unnecessary files..." -ForegroundColor Yellow

# Remove __pycache__ directories
Get-ChildItem -Path "python" -Recurse -Directory -Name "__pycache__" | ForEach-Object {
    Remove-Item -Recurse -Force "python\$_" -ErrorAction SilentlyContinue
}

# Remove compiled Python files
Get-ChildItem -Path "python" -Recurse -Include "*.pyc", "*.pyo", "*.pyd" | Remove-Item -Force -ErrorAction SilentlyContinue

# Remove shared libraries (not needed for Lambda)
Get-ChildItem -Path "python" -Recurse -Include "*.so", "*.dylib", "*.dll" | Remove-Item -Force -ErrorAction SilentlyContinue

# Remove Windows-specific packages and files
Write-Host "Removing Windows-specific packages..." -ForegroundColor Yellow
$WindowsPackages = @("pywin32", "adodbapi", "isapi", "pythonwin", "win32", "win32com", "win32comext", "pywin32_system32")
foreach ($package in $WindowsPackages) {
    $packagePath = "python\$package"
    if (Test-Path $packagePath) {
        Remove-Item -Recurse -Force $packagePath -ErrorAction SilentlyContinue
        Write-Host "  Removed: $package" -ForegroundColor Red
    }
}

# Remove Windows-specific files
Get-ChildItem -Path "python" -Recurse -Include "*.exe", "*.chm", "*.pth" | Remove-Item -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path "python" -Recurse -Include "*win32*", "*windows*" | Remove-Item -Force -ErrorAction SilentlyContinue

# Remove test directories and documentation
Get-ChildItem -Path "python" -Recurse -Directory | Where-Object { $_.Name -match "test|tests|doc|docs" } | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue
Get-ChildItem -Path "python" -Recurse -Include "*.md", "*.txt", "*.rst" | Remove-Item -Force -ErrorAction SilentlyContinue

# Fix OpenTelemetry entry points issue (must be done AFTER cleanup)
Write-Host "Fixing OpenTelemetry entry points..." -ForegroundColor Yellow
$entryPointsPath = "python\opentelemetry_api-1.36.0.dist-info\entry_points.txt"
if (Test-Path "python\opentelemetry_api-1.36.0.dist-info") {
    New-Item -Path $entryPointsPath -ItemType File -Force | Out-Null
    $entryPointsContent = @"
[opentelemetry_context]
contextvars_context = opentelemetry.context.contextvars_context:ContextVarsRuntimeContext
"@
    Set-Content -Path $entryPointsPath -Value $entryPointsContent -Encoding UTF8
    Write-Host "Created OpenTelemetry entry points file" -ForegroundColor Green
} else {
    Write-Host "OpenTelemetry distribution directory not found" -ForegroundColor Red
}

# Create the layer zip file
Write-Host "Creating layer zip file..." -ForegroundColor Cyan
# Remove existing layer.zip if it exists
Remove-Item -Path "layer.zip" -Force -ErrorAction SilentlyContinue

# Create zip with python directory structure
Compress-Archive -Path "python" -DestinationPath "layer.zip" -Force

# Get the size of the layer
$LayerSize = (Get-Item "layer.zip").Length
$LayerSizeMB = [math]::Round($LayerSize / 1MB, 2)
Write-Host "Layer created successfully: layer.zip (${LayerSizeMB} MB)" -ForegroundColor Green

# Verify the layer structure
Write-Host "Verifying layer structure..." -ForegroundColor Cyan
Add-Type -AssemblyName System.IO.Compression.FileSystem
$zip = [System.IO.Compression.ZipFile]::OpenRead("$PWD\layer.zip")
$entries = $zip.Entries | Select-Object -First 10
Write-Host "First 10 entries in layer:"
$entries | ForEach-Object { Write-Host "  $($_.FullName)" }
$zip.Dispose()

Write-Host "Lambda layer build completed successfully!" -ForegroundColor Green
