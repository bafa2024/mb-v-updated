# deploy-to-azure.ps1
param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$WebAppName,
    
    [Parameter(Mandatory=$true)]
    [string]$Location = "eastus"
)

# Login to Azure
Write-Host "Logging into Azure..." -ForegroundColor Yellow
az login

# Create resource group
Write-Host "Creating resource group..." -ForegroundColor Yellow
az group create --name $ResourceGroupName --location $Location

# Deploy ARM template
Write-Host "Deploying ARM template..." -ForegroundColor Yellow
az deployment group create `
    --resource-group $ResourceGroupName `
    --template-file azuredeploy.json `
    --parameters webAppName=$WebAppName

# Configure app settings
Write-Host "Configuring app settings..." -ForegroundColor Yellow
$settings = @(
    "MAPBOX_TOKEN=your_token_here",
    "MAPBOX_PUBLIC_TOKEN=your_public_token_here",
    "MAPBOX_USERNAME=your_username_here",
    "MAX_UPLOAD_SIZE=500",
    "SCM_DO_BUILD_DURING_DEPLOYMENT=true"
)

az webapp config appsettings set `
    --resource-group $ResourceGroupName `
    --name $WebAppName `
    --settings $settings

# Deploy code
Write-Host "Deploying application code..." -ForegroundColor Yellow
az webapp up `
    --resource-group $ResourceGroupName `
    --name $WebAppName `
    --runtime "PYTHON:3.11"

Write-Host "Deployment complete!" -ForegroundColor Green
Write-Host "App URL: https://$WebAppName.azurewebsites.net" -ForegroundColor Cyan
