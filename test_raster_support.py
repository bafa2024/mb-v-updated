#!/usr/bin/env python3
"""
Test if your Mapbox account supports raster-array tilesets
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()

def test_raster_support():
    """Check if account supports raster uploads"""
    
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    
    if not token or not username:
        print("❌ Missing MAPBOX_TOKEN or MAPBOX_USERNAME in .env")
        return False
    
    print("Testing Raster-Array Support")
    print("=" * 50)
    
    # Test upload credentials endpoint (Pro account feature)
    credentials_url = f"https://api.mapbox.com/uploads/v1/{username}/credentials?access_token={token}"
    
    print(f"Testing account: {username}")
    print(f"Token: {token[:10]}...{token[-10:]}")
    print()
    
    try:
        response = requests.post(credentials_url)
        
        print(f"Response status: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ Your account supports raster-array uploads!")
            print("   You have a Mapbox Pro account or higher")
            print("   You can create animated wind particle visualizations")
            
            # Show credentials info
            creds = response.json()
            print(f"\nUpload credentials received:")
            print(f"   Bucket: {creds.get('bucket', 'N/A')}")
            print(f"   Key: {creds.get('key', 'N/A')[:20]}...")
            print(f"   URL: {creds.get('url', 'N/A')[:50]}...")
            
            return True
            
        elif response.status_code == 422:
            print("❌ Your account does NOT support raster-array uploads")
            print("   You have a free Mapbox account")
            print("   You can only create static (vector) visualizations")
            print()
            print("To enable animated visualizations:")
            print("1. Go to https://account.mapbox.com/billing/")
            print("2. Upgrade to a Pro account")
            print("3. Then raster-array animations will work")
            
            return False
            
        elif response.status_code == 401:
            print("❌ Authentication failed")
            print("   Check that your token has the correct permissions")
            print("   Token needs: uploads:write, uploads:read")
            
            return False
            
        else:
            print(f"⚠️  Unexpected response: {response.status_code}")
            print(f"   Response: {response.text}")
            
            return False
            
    except Exception as e:
        print(f"❌ Error testing raster support: {e}")
        return False

def test_alternative_animation():
    """Suggest alternatives for free accounts"""
    
    print("\n" + "=" * 50)
    print("Alternative Animation Options for Free Accounts:")
    print("=" * 50)
    
    print("\n1. Use Mapbox's default wind layer (mapbox.gfs-winds)")
    print("   - This works on all accounts")
    print("   - Already included in your app")
    
    print("\n2. Create time-series animation with multiple vector layers")
    print("   - Upload multiple time steps as separate vector tilesets")
    print("   - Use JavaScript to toggle between them")
    
    print("\n3. Use client-side animation libraries")
    print("   - WindGL")
    print("   - Leaflet.Velocity")
    print("   - Custom Canvas/WebGL animation")
    
    print("\n4. Convert to video format")
    print("   - Create animation offline")
    print("   - Overlay video on map")

if __name__ == "__main__":
    supports_raster = test_raster_support()
    
    if not supports_raster:
        test_alternative_animation()
    
    print("\n" + "=" * 50)
    print("Summary:")
    if supports_raster:
        print("✅ Your account can create animated raster-array visualizations")
    else:
        print("❌ Your account is limited to static vector visualizations")
        print("💡 The app will automatically fall back to vector format")