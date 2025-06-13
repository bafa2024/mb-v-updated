#!/usr/bin/env python3
"""
Test script to verify raster tileset creation and fallback
"""

import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from mts_raster_manager import MTSRasterManager

load_dotenv()

async def test_raster_creation():
    """Test creating a raster tileset and handle fallback"""
    
    token = os.getenv("MAPBOX_TOKEN")
    username = os.getenv("MAPBOX_USERNAME")
    
    if not token or not username:
        print("❌ Missing MAPBOX_TOKEN or MAPBOX_USERNAME")
        return
    
    print("Testing Raster Tileset Creation")
    print("=" * 50)
    
    # Create a test NetCDF file path (you should have one)
    test_files = list(Path("uploads").glob("*.nc"))
    if not test_files:
        test_files = list(Path(".").glob("*.nc"))
    
    if not test_files:
        print("❌ No NetCDF files found to test with")
        print("Please upload a NetCDF file first or run: python create_netcdf.py")
        return
    
    test_file = test_files[0]
    print(f"Using test file: {test_file}")
    
    # Create raster manager
    manager = MTSRasterManager(token, username)
    
    # Try to create raster tileset
    print("\nAttempting to create raster-array tileset...")
    result = await manager.create_raster_tileset(str(test_file), "test_raster_fallback")
    
    print("\nResult:")
    print(f"Success: {result.get('success', False)}")
    
    if result.get('success'):
        print("✅ Raster tileset created successfully!")
        print(f"Tileset ID: {result.get('tileset_id')}")
        print(f"Format: {result.get('format')}")
    else:
        print("❌ Raster tileset creation failed")
        print(f"Error: {result.get('error')}")
        print(f"Error code: {result.get('error_code')}")
        print(f"Fallback to vector: {result.get('fallback_to_vector', False)}")
        
        if result.get('error_code') == 422:
            print("\n📌 This is expected for free accounts!")
            print("The app will automatically:")
            print("1. Create a vector tileset instead")
            print("2. Use client-side canvas animation")
            print("3. Provide the same smooth wind visualization")

if __name__ == "__main__":
    # First create a test NetCDF if needed
    if not list(Path(".").glob("*.nc")):
        print("Creating test NetCDF file first...")
        try:
            import subprocess
            subprocess.run([sys.executable, "create_netcdf.py"], check=True)
        except:
            print("Could not create test NetCDF. Please ensure create_netcdf.py exists")
    
    # Run the test
    asyncio.run(test_raster_creation())