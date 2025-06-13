#!/bin/bash
# deploy-to-do.sh - Deploy to DigitalOcean App Platform

# Install doctl if not present
if ! command -v doctl &> /dev/null; then
    echo "Installing doctl..."
    curl -sL https://github.com/digitalocean/doctl/releases/download/v1.94.0/doctl-1.94.0-windows-amd64.zip -o doctl.zip
    unzip doctl.zip
    rm doctl.zip
fi

# Authenticate (you need to have your DO token)
doctl auth init

# Create app
doctl apps create --spec do-app-spec.yaml

# List apps to get ID
doctl apps list

echo "Deployment initiated. Check DigitalOcean dashboard for status."
