#!/usr/bin/env python3
"""
Test recipe generation for NetCDF files
This will help verify the recipe is correct for Mapbox
"""

import json
import sys
from pathlib import Path
from utils.recipe_generator import create_enhanced_recipe_for_netcdf

def test_recipe_generation(nc_file: str):
    """Test recipe generation and display the result"""
    
    print(f"Testing recipe generation for: {nc_file}")
    print("="*60)
    
    try:
        # Generate recipe
        tileset_id = "test_weather_data"
        username = "your_username"
        
        recipe = create_enhanced_recipe_for_netcdf(nc_file, tileset_id, username)
        
        # Pretty print the recipe
        print("\nGenerated Recipe:")
        print(json.dumps(recipe, indent=2))
        
        # Validate recipe structure
        print("\n" + "="*60)
        print("Recipe Validation:")
        print(f"✓ Version: {recipe.get('version', 'MISSING')}")
        print(f"✓ Layers: {len(recipe.get('layers', {}))}")
        
        if 'metadata' in recipe:
            meta = recipe['metadata']
            print(f"✓ Variables: {len(meta.get('variables', []))}")
            print(f"✓ Vector pairs: {len(meta.get('vector_pairs', []))}")
            print(f"✓ Bands info: {len(meta.get('bands_info', {}))}")
        
        # Check for wind layers
        wind_layers = [l for l in recipe.get('layers', {}) if 'wind' in l.lower()]
        print(f"✓ Wind layers found: {len(wind_layers)}")
        
        # Save recipe to file
        output_file = Path(nc_file).stem + "_recipe.json"
        with open(output_file, 'w') as f:
            json.dump(recipe, f, indent=2)
        print(f"\n✓ Recipe saved to: {output_file}")
        
        return recipe
        
    except Exception as e:
        print(f"\n✗ Error generating recipe: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_recipe_generation.py <netcdf_file>")
        sys.exit(1)
    
    nc_file = sys.argv[1]
    if not Path(nc_file).exists():
        print(f"File not found: {nc_file}")
        sys.exit(1)
    
    test_recipe_generation(nc_file)