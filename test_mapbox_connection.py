#!/usr/bin/env python3
"""
Test Mapbox connection and credentials
Run this to verify your setup before uploading data
"""

import os
import requests
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

def test_mapbox_connection():
    """Test Mapbox API connection and permissions"""
    
    # Get credentials
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    public_token = os.getenv("MAPBOX_PUBLIC_TOKEN")
    
    print("Mapbox Connection Test")
    print("=" * 50)
    
    # Check environment variables
    if not token:
        print("❌ MAPBOX_TOKEN not set in .env file")
        return False
    else:
        print(f"✅ MAPBOX_TOKEN found: {token[:10]}...{token[-10:]}")
    
    if not username:
        print("❌ MAPBOX_USERNAME not set in .env file")
        return False
    else:
        print(f"✅ MAPBOX_USERNAME found: {username}")
    
    if not public_token:
        print("⚠️  MAPBOX_PUBLIC_TOKEN not set (using main token)")
    else:
        print(f"✅ MAPBOX_PUBLIC_TOKEN found: {public_token[:10]}...{public_token[-10:]}")
    
    print("\nTesting API Connection...")
    print("-" * 50)
    
    # Test 1: Check token validity
    print("\n1. Testing token validity...")
    token_url = f"https://api.mapbox.com/tokens/v2?access_token={token}"
    
    try:
        response = requests.get(token_url)
        if response.status_code == 200:
            token_info = response.json()
            if 'token' in token_info:
                scopes = token_info['token'].get('scopes', [])
                print(f"✅ Token is valid")
                print(f"   Scopes: {', '.join(scopes)}")
                
                # Check required scopes
                required_scopes = ['uploads:write', 'uploads:read', 'tilesets:write', 'tilesets:read']
                missing_scopes = [s for s in required_scopes if s not in scopes]
                
                if missing_scopes:
                    print(f"⚠️  Missing required scopes: {', '.join(missing_scopes)}")
                    print("   You need to create a new token with these scopes")
                else:
                    print("✅ All required scopes present")
        else:
            print(f"❌ Token validation failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error checking token: {e}")
        return False
    
    # Test 2: List tilesets
    print("\n2. Testing tileset listing...")
    tilesets_url = f"https://api.mapbox.com/tilesets/v1/{username}?access_token={token}&limit=5"
    
    try:
        response = requests.get(tilesets_url)
        if response.status_code == 200:
            tilesets = response.json()
            print(f"✅ Successfully listed tilesets")
            print(f"   Found {len(tilesets)} tilesets")
            
            # Show first few tilesets
            for ts in tilesets[:3]:
                print(f"   - {ts.get('id', 'Unknown')}: {ts.get('name', 'No name')}")
        else:
            print(f"❌ Failed to list tilesets: {response.status_code}")
            if response.status_code == 401:
                print("   Authentication failed - check your token")
    except Exception as e:
        print(f"❌ Error listing tilesets: {e}")
    
    # Test 3: Check upload permissions
    print("\n3. Testing upload permissions...")
    
    # Try to create a test source (we'll delete it immediately)
    test_source_id = "test_connection_check"
    source_url = f"https://api.mapbox.com/tilesets/v1/sources/{username}/{test_source_id}?access_token={token}"
    
    # First, try to delete any existing test source
    requests.delete(source_url)
    
    # Create a minimal test GeoJSON
    test_geojson = '{"type":"Feature","geometry":{"type":"Point","coordinates":[0,0]},"properties":{"test":1}}'
    
    files = {
        'file': ('test.json', test_geojson, 'application/x-ndjson')
    }
    
    try:
        response = requests.post(source_url, files=files)
        if response.status_code in [200, 201]:
            print("✅ Upload permissions working")
            # Clean up - delete the test source
            requests.delete(source_url)
        elif response.status_code == 401:
            print("❌ Upload authentication failed")
            print("   Make sure your token has 'uploads:write' scope")
        else:
            print(f"⚠️  Upload test returned: {response.status_code}")
            if response.text:
                print(f"   Response: {response.text}")
    except Exception as e:
        print(f"❌ Error testing uploads: {e}")
    
    # Test 4: Check for Pro account features (optional)
    print("\n4. Checking account features...")
    
    # Try to check upload credentials (Pro account feature)
    credentials_url = f"https://api.mapbox.com/uploads/v1/{username}/credentials?access_token={token}"
    
    try:
        response = requests.post(credentials_url)
        if response.status_code == 200:
            print("✅ Pro account features available (raster-array support)")
        elif response.status_code == 422:
            print("ℹ️  Free tier account (vector tilesets only)")
            print("   Raster-array animation requires Pro account")
        else:
            print(f"⚠️  Could not determine account tier: {response.status_code}")
    except Exception as e:
        print(f"⚠️  Error checking account features: {e}")
    
    print("\n" + "=" * 50)
    print("\nSummary:")
    print("--------")
    
    if token and username:
        print("✅ Basic configuration looks good")
        print("\nNext steps:")
        print("1. Make sure your token has all required scopes")
        print("2. Run the main application: python app.py")
        print("3. Upload a NetCDF file through the web interface")
        
        print("\nIf uploads fail, check:")
        print("- Token permissions (needs uploads:write, tilesets:write)")
        print("- File size (keep under 300MB for free tier)")
        print("- Valid NetCDF format with lat/lon coordinates")
        
        print("\nToken scope requirements:")
        print("- uploads:write - to upload source data")
        print("- uploads:read - to check upload status")
        print("- tilesets:write - to create and publish tilesets")
        print("- tilesets:read - to list and check tileset status")
        
        print("\nTo create a token with all scopes:")
        print("1. Go to https://account.mapbox.com/access-tokens/")
        print("2. Click 'Create a token'")
        print("3. Give it a name (e.g., 'Weather Visualization')")
        print("4. Under 'Public scopes', check:")
        print("   - styles:read")
        print("   - fonts:read")
        print("   - datasets:read")
        print("5. Under 'Secret scopes', check:")
        print("   - uploads:write")
        print("   - uploads:read")
        print("   - tilesets:write")
        print("   - tilesets:read")
        print("6. Click 'Create token'")
        print("7. Copy the token (starts with 'sk.') to your .env file")
    else:
        print("❌ Configuration incomplete")
        print("\nPlease set up your .env file with:")
        print("MAPBOX_TOKEN=your_secret_token")
        print("MAPBOX_USERNAME=your_username")
        print("MAPBOX_PUBLIC_TOKEN=your_public_token (optional)")
    
    return True

def test_specific_tileset(tileset_id):
    """Test access to a specific tileset"""
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    
    if not token or not username:
        print("❌ Missing credentials")
        return
    
    print(f"\nTesting specific tileset: {tileset_id}")
    print("-" * 50)
    
    # Get tileset info
    url = f"https://api.mapbox.com/tilesets/v1/{username}.{tileset_id}?access_token={token}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            info = response.json()
            print(f"✅ Tileset found: {info.get('name', 'No name')}")
            print(f"   Type: {info.get('type', 'Unknown')}")
            print(f"   Created: {info.get('created', 'Unknown')}")
            print(f"   Modified: {info.get('modified', 'Unknown')}")
            
            # Pretty print the full info
            print("\nFull tileset info:")
            print(json.dumps(info, indent=2))
        elif response.status_code == 404:
            print(f"❌ Tileset not found: {username}.{tileset_id}")
        else:
            print(f"❌ Error getting tileset: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    import sys
    
    # Run main test
    test_mapbox_connection()
    
    # If a tileset ID is provided as argument, test it specifically
    if len(sys.argv) > 1:
        tileset_id = sys.argv[1]
        test_specific_tileset(tileset_id)
        
    print("\n💡 Tip: You can test a specific tileset by running:")
    print("   python test_mapbox_connection.py <tileset_id>")