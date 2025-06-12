#!/usr/bin/env python3
"""
Debug script to check Mapbox account status and permissions
"""

import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def check_mapbox_account():
    """Check Mapbox account status and permissions"""
    
    # Get credentials
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    
    if not token or not username:
        print("❌ Missing MAPBOX_TOKEN or MAPBOX_USERNAME in .env file")
        return
    
    print(f"Checking account for username: {username}")
    print("=" * 50)
    
    # 1. Check token scopes
    print("\n1. Checking token scopes...")
    token_url = f"https://api.mapbox.com/tokens/v2?access_token={token}"
    
    try:
        response = requests.get(token_url)
        if response.status_code == 200:
            token_info = response.json()
            if 'token' in token_info:
                scopes = token_info['token'].get('scopes', [])
                print(f"   Token scopes: {', '.join(scopes)}")
                
                required_scopes = ['uploads:write', 'uploads:read', 'tilesets:write', 'tilesets:read']
                missing_scopes = [scope for scope in required_scopes if scope not in scopes]
                
                if missing_scopes:
                    print(f"   ⚠️  Missing required scopes: {', '.join(missing_scopes)}")
                else:
                    print("   ✅ All required scopes present")
        else:
            print(f"   ❌ Failed to check token: {response.status_code}")
    except Exception as e:
        print(f"   ❌ Error checking token: {e}")
    
    # 2. Test upload credentials endpoint
    print("\n2. Testing upload credentials (requires Pro account)...")
    cred_url = f"https://api.mapbox.com/uploads/v1/{username}/credentials?access_token={token}"
    
    try:
        response = requests.post(cred_url)
        if response.status_code == 200:
            print("   ✅ Upload credentials endpoint accessible")
            print("   This indicates Pro account features are available")
        elif response.status_code == 422:
            print("   ❌ Error 422: Account does not support raster uploads")
            print("   This means you need a Mapbox Pro account or higher")
        elif response.status_code == 401:
            print("   ❌ Error 401: Authentication failed")
            print("   Check your token permissions")
        else:
            print(f"   ❌ Unexpected response: {response.status_code}")
            print(f"   Response: {response.text}")
    except Exception as e:
        print(f"   ❌ Error testing credentials: {e}")
    
    # 3. Check account tier (if possible)
    print("\n3. Checking account information...")
    account_url = f"https://api.mapbox.com/accounts/v1/{username}?access_token={token}"
    
    try:
        response = requests.get(account_url)
        if response.status_code == 200:
            account_info = response.json()
            print(f"   Account ID: {account_info.get('id', 'Unknown')}")
            # Note: Account tier info might not be directly available via API
        else:
            print(f"   Could not retrieve account info: {response.status_code}")
    except Exception as e:
        print(f"   Error checking account: {e}")
    
    # 4. List recent uploads (to see if any succeeded)
    print("\n4. Checking recent uploads...")
    uploads_url = f"https://api.mapbox.com/uploads/v1/{username}?access_token={token}"
    
    try:
        response = requests.get(uploads_url)
        if response.status_code == 200:
            uploads = response.json()
            print(f"   Found {len(uploads)} recent uploads")
            
            # Check for any raster uploads
            raster_uploads = [u for u in uploads if u.get('tileset') and 'raster' in str(u.get('tileset', '')).lower()]
            if raster_uploads:
                print(f"   Found {len(raster_uploads)} raster uploads")
            else:
                print("   No raster uploads found")
        else:
            print(f"   Could not list uploads: {response.status_code}")
    except Exception as e:
        print(f"   Error listing uploads: {e}")
    
    print("\n" + "=" * 50)
    print("\nSUMMARY:")
    print("If you're seeing 422 errors for upload credentials, you need:")
    print("1. A Mapbox Pro account (or higher)")
    print("2. A token with uploads:write and tilesets:write scopes")
    print("\nVisit: https://account.mapbox.com/billing/ to check/upgrade your account")

if __name__ == "__main__":
    check_mapbox_account()