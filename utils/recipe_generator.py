"""
Recipe Generator for Mapbox Tilesets
Creates optimized recipes for weather data visualization
"""

import xarray as xr
import numpy as np
from typing import Dict, List, Any, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def create_enhanced_recipe_for_netcdf(netcdf_path: str, tileset_id: str, username: str) -> Dict[str, Any]:
    """
    Create a Mapbox recipe optimized for weather NetCDF data
    
    Args:
        netcdf_path: Path to NetCDF file
        tileset_id: ID for the tileset
        username: Mapbox username
        
    Returns:
        Recipe dictionary for Mapbox tileset creation
    """
    try:
        # Open and analyze NetCDF file
        ds = xr.open_dataset(netcdf_path)
        
        # Extract metadata
        metadata = extract_netcdf_metadata(ds)
        
        # Identify vector and scalar fields
        vector_pairs = identify_vector_pairs(ds)
        scalar_vars = identify_scalar_variables(ds, vector_pairs)
        
        # Create recipe
        recipe = {
            "version": 1,
            "layers": {},
            "metadata": {
                "tileset_id": tileset_id,
                "username": username,
                "created": np.datetime64('now').astype(str),
                "source_file": netcdf_path,
                "variables": list(ds.data_vars),
                "vector_pairs": vector_pairs,
                "scalar_variables": scalar_vars,
                "bands_info": {}
            }
        }
        
        # Add wind/vector layers
        band_index = 0
        for idx, pair in enumerate(vector_pairs):
            layer_name = f"{pair['name']}_layer"
            recipe["layers"][layer_name] = create_vector_layer(pair, idx, band_index)
            
            # Add band info to metadata
            u_stats = get_variable_stats(ds[pair['u']])
            v_stats = get_variable_stats(ds[pair['v']])
            
            recipe["metadata"]["bands_info"][f"{pair['name']}_u"] = {
                "band_index": band_index,
                "type": "vector_u",
                "stats": u_stats,
                "units": ds[pair['u']].attrs.get('units', 'unknown'),
                "long_name": ds[pair['u']].attrs.get('long_name', pair['u'])
            }
            band_index += 1
            
            recipe["metadata"]["bands_info"][f"{pair['name']}_v"] = {
                "band_index": band_index,
                "type": "vector_v", 
                "stats": v_stats,
                "units": ds[pair['v']].attrs.get('units', 'unknown'),
                "long_name": ds[pair['v']].attrs.get('long_name', pair['v'])
            }
            band_index += 1
        
        # Add scalar layers (temperature, pressure, etc.)
        for idx, var_name in enumerate(scalar_vars[:5]):  # Limit to first 5 scalar variables
            layer_name = f"{var_name}_layer"
            recipe["layers"][layer_name] = create_scalar_layer(var_name, ds[var_name], idx, band_index)
            
            # Add band info
            stats = get_variable_stats(ds[var_name])
            recipe["metadata"]["bands_info"][var_name] = {
                "band_index": band_index,
                "type": "scalar",
                "stats": stats,
                "units": ds[var_name].attrs.get('units', 'unknown'),
                "long_name": ds[var_name].attrs.get('long_name', var_name)
            }
            band_index += 1
        
        ds.close()
        return recipe
        
    except Exception as e:
        logger.error(f"Error creating recipe: {str(e)}")
        # Return a basic recipe on error
        return create_basic_recipe(tileset_id, username)


def extract_netcdf_metadata(ds: xr.Dataset) -> Dict[str, Any]:
    """Extract metadata from NetCDF dataset"""
    metadata = {
        "dimensions": dict(ds.dims),
        "coordinates": list(ds.coords),
        "variables": list(ds.data_vars),
        "attributes": dict(ds.attrs),
        "time_range": None,
        "spatial_extent": None
    }
    
    # Extract time range if available
    if 'time' in ds.coords:
        time_coord = ds.coords['time']
        metadata["time_range"] = {
            "start": str(time_coord.values[0]),
            "end": str(time_coord.values[-1]),
            "steps": len(time_coord)
        }
    
    # Extract spatial extent
    lat_names = ['lat', 'latitude', 'y']
    lon_names = ['lon', 'longitude', 'x']
    
    for lat_name in lat_names:
        if lat_name in ds.coords:
            lat = ds.coords[lat_name].values
            for lon_name in lon_names:
                if lon_name in ds.coords:
                    lon = ds.coords[lon_name].values
                    metadata["spatial_extent"] = {
                        "west": float(np.min(lon)),
                        "east": float(np.max(lon)),
                        "south": float(np.min(lat)),
                        "north": float(np.max(lat))
                    }
                    break
            break
    
    return metadata


def identify_vector_pairs(ds: xr.Dataset) -> List[Dict[str, str]]:
    """Identify u/v component pairs for vector fields"""
    vector_pairs = []
    processed_vars = set()
    
    # Common patterns for vector components
    patterns = [
        ('u', 'v'),
        ('u10', 'v10'),
        ('u_component', 'v_component'),
        ('u_wind', 'v_wind'),
        ('u_current', 'v_current'),
        ('water_u', 'water_v'),
        ('eastward', 'northward'),
        ('zonal', 'meridional')
    ]
    
    variables = list(ds.data_vars)
    
    for u_pattern, v_pattern in patterns:
        # Find matching pairs
        u_vars = [v for v in variables if u_pattern in v.lower() and v not in processed_vars]
        v_vars = [v for v in variables if v_pattern in v.lower() and v not in processed_vars]
        
        # Match pairs by common prefix/suffix
        for u_var in u_vars:
            for v_var in v_vars:
                # Check if they're a pair (same prefix or similar structure)
                if are_vector_pair(u_var, v_var, u_pattern, v_pattern):
                    name = "wind" if "wind" in u_var.lower() else "current" if "current" in u_var.lower() else "flow"
                    
                    # Check if at same vertical level
                    if 'level' in ds[u_var].dims and 'level' in ds[v_var].dims:
                        u_levels = ds[u_var].coords.get('level', [])
                        v_levels = ds[v_var].coords.get('level', [])
                        if len(u_levels) > 0 and len(v_levels) > 0:
                            name += f"_{int(u_levels.values[0])}"
                    
                    vector_pairs.append({
                        "name": name,
                        "u": u_var,
                        "v": v_var
                    })
                    processed_vars.add(u_var)
                    processed_vars.add(v_var)
                    break
    
    return vector_pairs


def are_vector_pair(u_var: str, v_var: str, u_pattern: str, v_pattern: str) -> bool:
    """Check if two variables form a vector pair"""
    # Remove the pattern to get the base name
    u_base = u_var.replace(u_pattern, '')
    v_base = v_var.replace(v_pattern, '')
    
    # They're a pair if the base names match
    return u_base == v_base or (u_var.replace('u', 'v') == v_var) or (u_var.replace('U', 'V') == v_var)


def identify_scalar_variables(ds: xr.Dataset, vector_pairs: List[Dict[str, str]]) -> List[str]:
    """Identify scalar variables (not part of vector pairs)"""
    vector_vars = set()
    for pair in vector_pairs:
        vector_vars.add(pair['u'])
        vector_vars.add(pair['v'])
    
    scalar_vars = []
    for var in ds.data_vars:
        if var not in vector_vars:
            # Check if it's a proper data variable (not auxiliary)
            if hasattr(ds[var], 'dims') and len(ds[var].dims) >= 2:
                scalar_vars.append(var)
    
    # Sort by importance (temperature, pressure, humidity, etc.)
    priority_patterns = ['temp', 'pressure', 'humid', 'precip', 'cloud', 'rain', 'snow']
    
    def get_priority(var_name):
        var_lower = var_name.lower()
        for i, pattern in enumerate(priority_patterns):
            if pattern in var_lower:
                return i
        return len(priority_patterns)
    
    scalar_vars.sort(key=get_priority)
    return scalar_vars


def get_variable_stats(data_var: xr.DataArray) -> Dict[str, float]:
    """Get statistics for a variable"""
    try:
        # If has time dimension, use first timestep
        if 'time' in data_var.dims:
            data_var = data_var.isel(time=0)
        
        # Compute stats
        values = data_var.values
        valid_values = values[~np.isnan(values)]
        
        if len(valid_values) > 0:
            return {
                "min": float(np.min(valid_values)),
                "max": float(np.max(valid_values)),
                "mean": float(np.mean(valid_values)),
                "std": float(np.std(valid_values))
            }
    except Exception as e:
        logger.warning(f"Could not compute stats: {e}")
    
    return {"min": 0, "max": 1, "mean": 0.5, "std": 0.1}


def create_vector_layer(pair: Dict[str, str], index: int, band_start: int) -> Dict[str, Any]:
    """Create a vector layer configuration for wind/current"""
    return {
        "minzoom": 0,
        "maxzoom": 10,
        "fields": {
            f"{pair['name']}_u": {
                "band": band_start,
                "type": "number"
            },
            f"{pair['name']}_v": {
                "band": band_start + 1,
                "type": "number"
            },
            f"{pair['name']}_speed": {
                "expression": f"sqrt({pair['name']}_u * {pair['name']}_u + {pair['name']}_v * {pair['name']}_v)",
                "type": "number"
            },
            f"{pair['name']}_direction": {
                "expression": f"atan2({pair['name']}_v, {pair['name']}_u) * 180 / 3.14159",
                "type": "number"
            }
        }
    }


def create_scalar_layer(var_name: str, data_var: xr.DataArray, index: int, band_index: int) -> Dict[str, Any]:
    """Create a scalar layer configuration"""
    stats = get_variable_stats(data_var)
    
    # Extract min and max values to avoid f-string syntax issues
    min_val = stats['min']
    max_val = stats['max']
    
    return {
        "minzoom": 0,
        "maxzoom": 10,
        "fields": {
            var_name: {
                "band": band_index,
                "type": "number"
            },
            f"{var_name}_normalized": {
                "expression": f"({var_name} - {min_val}) / ({max_val} - {min_val})",
                "type": "number"
            }
        }
    }


def create_basic_recipe(tileset_id: str, username: str) -> Dict[str, Any]:
    """Create a basic recipe when advanced analysis fails"""
    return {
        "version": 1,
        "layers": {
            "default_layer": {
                "minzoom": 0,
                "maxzoom": 10,
                "fields": {}
            }
        },
        "metadata": {
            "tileset_id": tileset_id,
            "username": username,
            "created": np.datetime64('now').astype(str),
            "error": "Failed to analyze NetCDF file, using basic recipe"
        }
    }