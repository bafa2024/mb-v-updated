#!/usr/bin/env python3
"""
Debug script to check existing tilesets and their formats
"""

import os
import requests
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def check_account_capabilities():
    """Check what your Mapbox account can do"""
    
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    
    if not token or not username:
        print("❌ Missing MAPBOX_TOKEN or MAPBOX_USERNAME")
        return
    
    print(f"Checking account: {username}")
    print("=" * 50)
    
    # Test raster upload capability
    print("\n1. Testing raster upload capability...")
    cred_url = f"https://api.mapbox.com/uploads/v1/{username}/credentials?access_token={token}"
    
    try:
        response = requests.post(cred_url)
        print(f"   Response status: {response.status_code}")
        
        if response.status_code == 200:
            print("   ✅ Your account supports raster uploads (Pro account)")
        elif response.status_code == 422:
            print("   ❌ Your account does NOT support raster uploads (Free account)")
            print("   → Will use client-side animation instead")
        else:
            print(f"   ⚠️  Unexpected response: {response.status_code}")
            if response.text:
                print(f"   Response: {response.text}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    # List existing tilesets
    print("\n2. Checking existing tilesets...")
    list_url = f"https://api.mapbox.com/tilesets/v1/{username}?access_token={token}&limit=20"
    
    try:
        response = requests.get(list_url)
        if response.status_code == 200:
            tilesets = response.json()
            print(f"   Found {len(tilesets)} tilesets")
            
            # Check each tileset
            for ts in tilesets:
                if any(keyword in ts.get('name', '').lower() or keyword in ts.get('id', '').lower() 
                      for keyword in ['weather', 'wind', 'netcdf', 'wx_']):
                    print(f"\n   Tileset: {ts['id']}")
                    print(f"   Name: {ts.get('name', 'N/A')}")
                    print(f"   Type: {ts.get('type', 'N/A')}")
                    
                    # Check detailed info
                    detail_url = f"https://api.mapbox.com/tilesets/v1/{ts['id']}?access_token={token}"
                    detail_response = requests.get(detail_url)
                    if detail_response.status_code == 200:
                        details = detail_response.json()
                        print(f"   Format: {details.get('type', 'unknown')}")
                        if 'layers' in details:
                            print(f"   Layers: {len(details['layers'])}")
    except Exception as e:
        print(f"   ❌ Error listing tilesets: {e}")
    
    # Check recipe files
    print("\n3. Checking recipe files...")
    recipe_dir = Path("recipes")
    if recipe_dir.exists():
        recipe_files = list(recipe_dir.glob("*.json"))
        print(f"   Found {len(recipe_files)} recipe files")
        
        for recipe_file in recipe_files[:5]:  # Show first 5
            try:
                with open(recipe_file, 'r') as f:
                    recipe = json.load(f)
                    print(f"\n   Recipe: {recipe_file.name}")
                    print(f"   Format: {recipe.get('format', 'N/A')}")
                    print(f"   Actual format: {recipe.get('actual_format', 'N/A')}")
                    print(f"   Requested format: {recipe.get('requested_format', 'N/A')}")
                    print(f"   Use client animation: {recipe.get('use_client_animation', False)}")
            except Exception as e:
                print(f"   Error reading {recipe_file}: {e}")
    
    print("\n" + "=" * 50)
    print("\nSUMMARY:")
    print("If you see 422 errors above, your free account will automatically:")
    print("1. Create vector tilesets instead of raster")
    print("2. Use client-side canvas animation for smooth visualization")
    print("3. Show the same visual result as Pro accounts")
    print("\nThe app will handle this automatically!")

if __name__ == "__main__":
    check_account_capabilities()