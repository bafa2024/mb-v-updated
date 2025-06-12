#!/usr/bin/env python3
"""Test script to verify the Weather Visualization Platform setup"""

import os
import sys
from pathlib import Path
import importlib.util

def test_setup():
    print("Testing Weather Visualization Setup...")
    print("=" * 50)
    
    # Check Python version
    print(f"Python version: {sys.version}")
    if sys.version_info < (3, 7):
        print("⚠️  Warning: Python 3.7+ recommended")
    else:
        print("✓ Python version OK")
    
    print("\n" + "=" * 50)
    print("Checking required directories...")
    
    # Check required directories
    required_dirs = ['static', 'templates', 'uploads', 'processed', 'recipes']
    for dir_name in required_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            print(f"✓ {dir_name}/ directory exists")
        else:
            print(f"✗ {dir_name}/ directory missing - creating...")
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
                print(f"  ✓ Created {dir_name}/ directory")
            except Exception as e:
                print(f"  ✗ Failed to create {dir_name}/: {e}")
    
    print("\n" + "=" * 50)
    print("Checking environment variables...")
    
    # Check environment variables
    env_vars = {
        'MAPBOX_TOKEN': 'Secret Mapbox token (with uploads:write, tilesets:write scopes)',
        'MAPBOX_PUBLIC_TOKEN': 'Public Mapbox token (for map display)',
        'MAPBOX_USERNAME': 'Your Mapbox username'
    }
    
    env_ok = True
    for var, description in env_vars.items():
        value = os.getenv(var)
        if value:
            # Mask token values for security
            if 'TOKEN' in var:
                masked_value = value[:10] + '...' + value[-10:] if len(value) > 20 else 'SET'
                print(f"✓ {var} is set: {masked_value}")
            else:
                print(f"✓ {var} is set: {value}")
        else:
            print(f"✗ {var} is not set - {description}")
            env_ok = False
    
    if not env_ok:
        print("\n⚠️  Create a .env file with the missing variables")
    
    print("\n" + "=" * 50)
    print("Checking static files...")
    
    # Check if static files exist
    static_files = [
        ('static/style.css', 'CSS styles'),
        ('templates/main_weather_map.html', 'Main HTML template')
    ]
    
    for file_path, description in static_files:
        if Path(file_path).exists():
            file_size = Path(file_path).stat().st_size
            print(f"✓ {file_path} exists ({file_size} bytes)")
        else:
            print(f"✗ {file_path} missing - {description}")
    
    print("\n" + "=" * 50)
    print("Checking Python packages...")
    
    # Check required packages
    packages = {
        'fastapi': 'FastAPI web framework',
        'uvicorn': 'ASGI server',
        'xarray': 'NetCDF data handling',
        'numpy': 'Numerical operations',
        'aiofiles': 'Async file operations',
        'requests': 'HTTP requests',
        'jinja2': 'Template engine'
    }
    
    for package, description in packages.items():
        spec = importlib.util.find_spec(package)
        if spec is not None:
            print(f"✓ {package} installed - {description}")
        else:
            print(f"✗ {package} not installed - {description}")
            print(f"  Run: pip install {package}")
    
    print("\n" + "=" * 50)
    print("Checking Mapbox connection...")
    
    # Try to import and test Mapbox connection
    token = os.getenv('MAPBOX_TOKEN')
    username = os.getenv('MAPBOX_USERNAME')
    
    if token and username:
        try:
            import requests
            # Test API connection
            url = f"https://api.mapbox.com/tilesets/v1/{username}?access_token={token}&limit=1"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                print("✓ Mapbox API connection successful")
            elif response.status_code == 401:
                print("✗ Mapbox authentication failed - check your token")
            else:
                print(f"⚠️  Mapbox API returned status {response.status_code}")
                
        except Exception as e:
            print(f"✗ Could not test Mapbox connection: {e}")
    else:
        print("✗ Cannot test Mapbox - credentials not set")
    
    print("\n" + "=" * 50)
    print("Checking file permissions...")
    
    # Check write permissions
    test_dirs = ['uploads', 'processed', 'recipes']
    for dir_name in test_dirs:
        dir_path = Path(dir_name)
        if dir_path.exists():
            try:
                test_file = dir_path / '.test_write'
                test_file.write_text('test')
                test_file.unlink()
                print(f"✓ Write permission OK for {dir_name}/")
            except Exception as e:
                print(f"✗ No write permission for {dir_name}/: {e}")
    
    print("\n" + "=" * 50)
    print("\nSetup test complete!")
    print("\nNext steps:")
    print("1. Ensure all required packages are installed:")
    print("   pip install -r requirements.txt")
    print("\n2. Create a .env file with your Mapbox credentials:")
    print("   MAPBOX_TOKEN=your_secret_token")
    print("   MAPBOX_PUBLIC_TOKEN=your_public_token")
    print("   MAPBOX_USERNAME=your_username")
    print("\n3. Run the application:")
    print("   python app.py")
    print("\n4. Open http://localhost:8000 in your browser")
    
    # Check for common issues
    print("\n" + "=" * 50)
    print("Common issues and solutions:")
    print("\n1. If you see a white page:")
    print("   - Check that static/style.css exists")
    print("   - Check browser console for JavaScript errors")
    print("   - Verify Mapbox tokens are set correctly")
    print("\n2. If uploads fail:")
    print("   - Check write permissions for uploads/ directory")
    print("   - Verify file size is under 500MB")
    print("   - Check logs for specific error messages")
    print("\n3. If raster-array fails:")
    print("   - This requires a Mapbox Pro account")
    print("   - Use Vector format instead (works on free tier)")
    print("\n4. For Windows users:")
    print("   - The app includes Windows path fixes")
    print("   - Use forward slashes in file paths")

if __name__ == "__main__":
    test_setup()