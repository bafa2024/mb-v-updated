#!/usr/bin/env python3
"""
Setup script for Weather Visualization Platform
Creates necessary directories and checks configuration
"""

import os
import sys
from pathlib import Path

def setup_environment():
    """Set up the environment for the weather visualization platform"""
    
    print("Weather Visualization Platform - Setup")
    print("=" * 50)
    
    # Create necessary directories
    directories = [
        'uploads',
        'processed',
        'recipes',
        'static',
        'templates'
    ]
    
    print("\nCreating directories...")
    for dir_name in directories:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created {dir_name}/")
        else:
            print(f"✓ {dir_name}/ already exists")
    
    # Check for .env file
    print("\nChecking configuration...")
    env_path = Path('.env')
    
    if not env_path.exists():
        print("\n⚠️  No .env file found. Creating template...")
        
        env_template = """# Mapbox Configuration
MAPBOX_TOKEN=your_secret_mapbox_token_here
MAPBOX_PUBLIC_TOKEN=your_public_mapbox_token_here
MAPBOX_USERNAME=your_mapbox_username_here

# Optional: AWS Configuration (not required for basic usage)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1

# Application Configuration
APP_ENV=development
DEBUG=True
LOG_LEVEL=INFO

# File Upload Limits (in MB)
MAX_UPLOAD_SIZE=500

# Tile Processing
DEFAULT_TILE_SIZE=512
MAX_ZOOM_LEVEL=10
MIN_ZOOM_LEVEL=0
"""
        
        with open('.env', 'w') as f:
            f.write(env_template)
        
        print("✅ Created .env template file")
        print("\n⚠️  IMPORTANT: Edit .env file and add your Mapbox credentials:")
        print("   1. Go to https://account.mapbox.com/access-tokens/")
        print("   2. Create a new token with these scopes:")
        print("      - uploads:write")
        print("      - uploads:read")
        print("      - tilesets:write")
        print("      - tilesets:read")
        print("   3. Add your token and username to the .env file")
    else:
        print("✅ .env file found")
        
        # Check if credentials are set
        from dotenv import load_dotenv
        load_dotenv()
        
        if os.getenv('MAPBOX_TOKEN') and os.getenv('MAPBOX_TOKEN') != 'your_secret_mapbox_token_here':
            print("✅ Mapbox token configured")
        else:
            print("⚠️  Mapbox token not configured in .env")
        
        if os.getenv('MAPBOX_USERNAME') and os.getenv('MAPBOX_USERNAME') != 'your_mapbox_username_here':
            print("✅ Mapbox username configured")
        else:
            print("⚠️  Mapbox username not configured in .env")
    
    # Check Python version
    print(f"\nPython version: {sys.version}")
    if sys.version_info < (3, 7):
        print("⚠️  Python 3.7+ is required")
    else:
        print("✅ Python version OK")
    
    # Check for required files
    print("\nChecking required files...")
    required_files = [
        'app.py',
        'tileset_management.py',
        'requirements.txt'
    ]
    
    for file_name in required_files:
        if Path(file_name).exists():
            print(f"✅ {file_name} found")
        else:
            print(f"❌ {file_name} missing")
    
    print("\n" + "=" * 50)
    print("\nSetup complete!")
    print("\nNext steps:")
    print("1. Install dependencies: pip install -r requirements.txt")
    print("2. Configure your Mapbox credentials in .env")
    print("3. Run the test script: python test_mapbox_connection.py")
    print("4. Start the application: python app.py")
    print("5. Open http://localhost:8000 in your browser")

if __name__ == "__main__":
    setup_environment()