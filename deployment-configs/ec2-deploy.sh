#!/bin/bash
# ec2-deploy.sh - EC2 deployment script

# Update system
sudo apt update && sudo apt upgrade -y

# Install Python 3.11
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev -y

# Install other dependencies
sudo apt install nginx supervisor git -y

# Setup application
cd /var/www
sudo git clone https://github.com/yourusername/weather-visualization.git
cd weather-visualization
sudo chown -R ubuntu:ubuntu .

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Create .env file (edit this with your values)
cat > .env << EOF
MAPBOX_TOKEN=your_token_here
MAPBOX_PUBLIC_TOKEN=your_public_token_here
MAPBOX_USERNAME=your_username_here
MAX_UPLOAD_SIZE=500
DEBUG=False
LOG_LEVEL=INFO
EOF

# Setup directories
python setup_environment.py
