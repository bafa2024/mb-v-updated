# utils/raster_array_converter.py
"""
Enhanced Raster Array Converter for Mapbox Wind Particle Animation
Converts NetCDF to multi-band Cloud Optimized GeoTIFF for raster-array tilesets
"""

import numpy as np
import xarray as xr
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_bounds
from rasterio.enums import Resampling
import tempfile
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict, Any

logger = logging.getLogger(__name__)


class RasterArrayConverter:
    """Converts NetCDF wind data to raster-array format for Mapbox particle animation"""
    
    @staticmethod
    def netcdf_to_raster_array(netcdf_path: str, output_path: str) -> Dict[str, Any]:
        """
        Convert NetCDF to multi-band GeoTIFF optimized for Mapbox raster-array
        
        Returns:
            Dict with success status and metadata
        """
        try:
            # Open NetCDF
            ds = xr.open_dataset(netcdf_path)
            
            # Find wind components
            u_var, v_var = RasterArrayConverter._find_wind_components(ds)
            if not u_var or not v_var:
                raise ValueError("Could not find U/V wind components in NetCDF")
            
            logger.info(f"Found wind components: U={u_var}, V={v_var}")
            
            # Get coordinate info
            lons, lats = RasterArrayConverter._get_coordinates(ds)
            
            # Get wind data
            u_data = ds[u_var]
            v_data = ds[v_var]
            
            # Handle time dimension - take first timestep
            if 'time' in u_data.dims:
                u_data = u_data.isel(time=0)
                v_data = v_data.isel(time=0)
            
            # Get data as numpy arrays
            u_array = u_data.values
            v_array = v_data.values
            
            # Handle NaN values
            u_array = np.nan_to_num(u_array, nan=0.0)
            v_array = np.nan_to_num(v_array, nan=0.0)
            
            # Ensure data is float32 for Mapbox
            u_array = u_array.astype(np.float32)
            v_array = v_array.astype(np.float32)
            
            # Create georeferencing
            height, width = u_array.shape
            west, east = float(np.min(lons)), float(np.max(lons))
            south, north = float(np.min(lats)), float(np.max(lats))
            
            transform = from_bounds(west, south, east, north, width, height)
            
            # Calculate magnitude for third band (optional but helpful)
            magnitude = np.sqrt(u_array**2 + v_array**2).astype(np.float32)
            
            # Create multi-band raster
            with rasterio.open(
                output_path,
                'w',
                driver='GTiff',
                height=height,
                width=width,
                count=3,  # 3 bands: U, V, magnitude
                dtype='float32',
                crs=CRS.from_epsg(4326),
                transform=transform,
                compress='deflate',
                tiled=True,
                blockxsize=512,
                blockysize=512,
                BIGTIFF='IF_SAFER'
            ) as dst:
                # Write bands
                dst.write(u_array, 1)
                dst.write(v_array, 2)
                dst.write(magnitude, 3)
                
                # Set band descriptions
                dst.set_band_description(1, 'u_component')
                dst.set_band_description(2, 'v_component')
                dst.set_band_description(3, 'magnitude')
                
                # Add metadata
                dst.update_tags(
                    u_units=str(u_data.attrs.get('units', 'm/s')),
                    v_units=str(v_data.attrs.get('units', 'm/s'))
                )
            
            # Close dataset
            ds.close()
            
            # Get statistics
            stats = {
                'u_min': float(np.min(u_array)),
                'u_max': float(np.max(u_array)),
                'v_min': float(np.min(v_array)),
                'v_max': float(np.max(v_array)),
                'speed_max': float(np.max(magnitude))
            }
            
            logger.info(f"Created raster array GeoTIFF: {output_path}")
            logger.info(f"Statistics: {stats}")
            
            return {
                'success': True,
                'output_path': output_path,
                'bounds': {'west': west, 'east': east, 'south': south, 'north': north},
                'shape': {'width': width, 'height': height},
                'bands': 3,
                'stats': stats
            }
            
        except Exception as e:
            logger.error(f"Error converting to raster array: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }
    
    @staticmethod
    def create_cog(input_path: str, output_path: str) -> bool:
        """Convert GeoTIFF to Cloud Optimized GeoTIFF"""
        try:
            with rasterio.open(input_path) as src:
                profile = src.profile.copy()
                
                # Update profile for COG
                profile.update({
                    'driver': 'GTiff',
                    'TILED': 'YES',
                    'COMPRESS': 'DEFLATE',
                    'BLOCKXSIZE': 512,
                    'BLOCKYSIZE': 512,
                    'BIGTIFF': 'IF_SAFER',
                    'INTERLEAVE': 'BAND'
                })
                
                # Read all bands
                data = src.read()
                
                # Write COG with overviews
                with rasterio.open(output_path, 'w', **profile) as dst:
                    dst.write(data)
                    
                    # Build overviews
                    factors = [2, 4, 8, 16, 32]
                    dst.build_overviews(factors, Resampling.average)
                    dst.update_tags(ns='rio_overview', resampling='average')
            
            logger.info(f"Created Cloud Optimized GeoTIFF: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating COG: {str(e)}")
            return False
    
    @staticmethod
    def _find_wind_components(ds) -> Tuple[Optional[str], Optional[str]]:
        """Find U and V wind component variable names"""
        u_patterns = ['u', 'u10', 'u_wind', 'u_component', 'eastward', 'ugrd', 'uas', 'U10', 'u-wind']
        v_patterns = ['v', 'v10', 'v_wind', 'v_component', 'northward', 'vgrd', 'vas', 'V10', 'v-wind']
        
        u_var = None
        v_var = None
        
        for var in ds.data_vars:
            var_lower = var.lower()
            
            if not u_var:
                for pattern in u_patterns:
                    if pattern.lower() in var_lower:
                        u_var = var
                        break
            
            if not v_var:
                for pattern in v_patterns:
                    if pattern.lower() in var_lower:
                        v_var = var
                        break
            
            if u_var and v_var:
                break
        
        return u_var, v_var
    
    @staticmethod
    def _get_coordinates(ds) -> Tuple[np.ndarray, np.ndarray]:
        """Extract longitude and latitude arrays"""
        lon_names = ['lon', 'longitude', 'x', 'X', 'long']
        lat_names = ['lat', 'latitude', 'y', 'Y']
        
        lons = None
        lats = None
        
        # Check coordinates
        for name in lon_names:
            if name in ds.coords:
                lons = ds.coords[name].values
                break
        
        for name in lat_names:
            if name in ds.coords:
                lats = ds.coords[name].values
                break
        
        if lons is None or lats is None:
            raise ValueError("Could not find longitude/latitude coordinates")
        
        return lons, lats


def create_raster_array_recipe(username: str, tileset_id: str) -> Dict[str, Any]:
    """Create a recipe specifically for raster-array wind animation"""
    
    recipe = {
        "version": 1,
        "type": "rasterarray",
        "sources": [
            {
                "type": "raster",
                "encoding": "terrarium",
                "tiles": [f"https://api.mapbox.com/rasterarray/v1/{username}.{tileset_id}/{{z}}/{{x}}/{{y}}.png"]
            }
        ],
        "layers": {
            "wind": {
                "type": "raster",
                "source": "raster",
                "tileSize": 512,
                "encoding": "terrarium"
            }
        }
    }
    
    return recipe


def validate_raster_for_particles(geotiff_path: str) -> Dict[str, Any]:
    """Validate that a GeoTIFF is suitable for particle animation"""
    try:
        with rasterio.open(geotiff_path) as src:
            # Check band count
            if src.count < 2:
                return {
                    'valid': False,
                    'error': 'Need at least 2 bands (U and V components)'
                }
            
            # Check data type
            if src.dtypes[0] != 'float32':
                return {
                    'valid': False,
                    'error': 'Data must be float32'
                }
            
            # Check CRS
            if not src.crs or src.crs.to_epsg() != 4326:
                return {
                    'valid': False,
                    'error': 'CRS must be EPSG:4326'
                }
            
            # Get statistics
            stats = []
            for i in range(1, min(3, src.count + 1)):
                band = src.read(i)
                stats.append({
                    'band': i,
                    'min': float(np.min(band)),
                    'max': float(np.max(band)),
                    'mean': float(np.mean(band))
                })
            
            return {
                'valid': True,
                'bands': src.count,
                'shape': (src.width, src.height),
                'bounds': src.bounds,
                'stats': stats
            }
            
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }