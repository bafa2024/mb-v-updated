"""
Mapbox Tileset Management Module - Fixed Version
Handles creation and management of Mapbox tilesets from NetCDF data
Fixed: Proper file upload handling for tileset sources
"""

import os
import json
import logging
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import requests
from datetime import datetime
import xarray as xr
import numpy as np
import time

logger = logging.getLogger(__name__)


class MapboxTilesetManager:
    """Manages Mapbox tileset operations"""
    
    def __init__(self, access_token: str, username: str):
        self.access_token = access_token
        self.username = username
        self.api_base = "https://api.mapbox.com"
        
    def create_raster_array_tileset(self, netcdf_path: str, tileset_id: str) -> Dict[str, Any]:
        """
        Attempt to create raster-array tileset (requires pro account)
        Falls back to vector format if raster upload fails
        """
        logger.info("Attempting raster-array tileset creation...")
        
        try:
            # Try to convert to GeoTIFF and upload
            from utils.raster_converter import RasterConverter
            
            # Convert NetCDF to GeoTIFF
            geotiff_path = tempfile.mktemp(suffix='.tif')
            success = RasterConverter.netcdf_to_geotiff(netcdf_path, geotiff_path)
            
            if not success:
                logger.warning("Failed to convert to GeoTIFF, falling back to vector format")
                return self.process_netcdf_to_tileset(netcdf_path, tileset_id, {})
            
            # Try to upload as raster
            result = self._upload_raster_tileset(geotiff_path, tileset_id)
            
            # Clean up
            if os.path.exists(geotiff_path):
                os.remove(geotiff_path)
            
            if result['success']:
                return result
            else:
                logger.warning(f"Raster upload failed: {result['error']}, falling back to vector format")
                return self.process_netcdf_to_tileset(netcdf_path, tileset_id, {})
                
        except ImportError:
            logger.warning("Raster converter not available, using vector format")
            return self.process_netcdf_to_tileset(netcdf_path, tileset_id, {})
        except Exception as e:
            logger.error(f"Error in raster tileset creation: {str(e)}")
            return self.process_netcdf_to_tileset(netcdf_path, tileset_id, {})
    
    def _upload_raster_tileset(self, geotiff_path: str, tileset_id: str) -> Dict[str, Any]:
        """Try to upload raster tileset (may fail on free accounts)"""
        try:
            # Get upload credentials
            credentials_url = f"{self.api_base}/uploads/v1/{self.username}?access_token={self.access_token}"
            
            response = requests.post(credentials_url, json={})
            
            if response.status_code != 200:
                return {
                    "success": False,
                    "error": f"Failed to get upload credentials: {response.status_code}"
                }
            
            credentials = response.json()
            
            # Upload to S3
            with open(geotiff_path, 'rb') as f:
                files = {'file': (os.path.basename(geotiff_path), f)}
                upload_response = requests.post(credentials['url'], files=files)
            
            if upload_response.status_code not in [200, 201, 204]:
                return {
                    "success": False,
                    "error": "Failed to upload to S3"
                }
            
            # Create tileset
            tileset_url = f"{self.api_base}/uploads/v1/{self.username}/{tileset_id}?access_token={self.access_token}"
            
            tileset_data = {
                "url": credentials['url'],
                "tileset": f"{self.username}.{tileset_id}",
                "name": f"Wind data {tileset_id}"
            }
            
            create_response = requests.post(tileset_url, json=tileset_data)
            
            if create_response.status_code in [200, 201, 202]:
                return {
                    "success": True,
                    "tileset_id": f"{self.username}.{tileset_id}",
                    "format": "raster-array"
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to create tileset: {create_response.text}"
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def process_netcdf_to_tileset(self, netcdf_path: str, tileset_id: str, recipe: Dict) -> Dict[str, Any]:
        """Process NetCDF to vector tileset (reliable method)"""
        try:
            # Step 1: Convert NetCDF to line-delimited GeoJSON
            logger.info("Converting NetCDF to GeoJSON...")
            geojson_path = self._convert_netcdf_to_geojson(netcdf_path)
            
            if not geojson_path:
                return {"success": False, "error": "Failed to convert NetCDF to GeoJSON"}
            
            # Verify the file exists and has content
            if not os.path.exists(geojson_path):
                return {"success": False, "error": "GeoJSON file was not created"}
                
            file_size = os.path.getsize(geojson_path)
            logger.info(f"GeoJSON file created: {geojson_path} ({file_size} bytes)")
            
            if file_size == 0:
                return {"success": False, "error": "GeoJSON file is empty"}
            
            # Step 2: Create tileset source
            source_id = f"{tileset_id}_src"
            source_id = self._sanitize_id(source_id)
            
            logger.info(f"Creating tileset source: {source_id}")
            source_result = self.create_tileset_source(source_id, geojson_path)
            
            # Clean up GeoJSON file
            if os.path.exists(geojson_path):
                os.remove(geojson_path)
            
            if not source_result["success"]:
                return source_result
            
            # Step 3: Create tileset with recipe
            tileset_id = self._sanitize_id(tileset_id)
            
            recipe = {
                "version": 1,
                "layers": {
                    "weather_data": {
                        "source": f"mapbox://tileset-source/{self.username}/{source_id}",
                        "minzoom": 0,
                        "maxzoom": 10,
                        "features": {
                            "attributes": {
                                "allowed_output": ["speed", "direction", "u", "v"]
                            }
                        }
                    }
                }
            }
            
            logger.info(f"Creating tileset: {tileset_id}")
            tileset_result = self.create_tileset(tileset_id, recipe)
            
            if not tileset_result["success"]:
                return tileset_result
            
            # Step 4: Publish tileset
            logger.info(f"Publishing tileset: {tileset_id}")
            publish_result = self.publish_tileset(tileset_id)
            
            if not publish_result["success"]:
                return publish_result
            
            return {
                "success": True,
                "tileset_id": f"{self.username}.{tileset_id}",
                "job_id": publish_result.get("job_id"),
                "format": "vector"
            }
            
        except Exception as e:
            logger.error(f"Error processing NetCDF: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def create_tileset_source(self, source_id: str, file_path: str) -> Dict[str, Any]:
        """Upload source data to Mapbox - FIXED VERSION"""
        try:
            url = f"{self.api_base}/tilesets/v1/sources/{self.username}/{source_id}?access_token={self.access_token}"
            
            # Read file content as line-delimited JSON
            features = []
            with open(file_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        features.append(line)
            
            if not features:
                return {"success": False, "error": "No features found in GeoJSON file"}
                
            # Join features with newlines for NDJSON format
            ndjson_content = '\n'.join(features)
            
            logger.info(f"Uploading source with {len(features)} features...")
            
            # Make request with proper content type and multipart form data
            files = {
                'file': (f'{source_id}.ndjson', ndjson_content.encode('utf-8'), 'application/x-ndjson')
            }
            
            response = requests.put(url, files=files)
            
            logger.info(f"Source upload response: {response.status_code}")
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset source: {source_id}")
                return {"success": True, "source_id": f"{self.username}.{source_id}"}
            else:
                error_msg = f"Failed to create source: {response.status_code} - {response.text}"
                logger.error(error_msg)
                
                # Provide helpful error messages
                if response.status_code == 400 and "No file data" in response.text:
                    error_msg = "Failed to upload source data. The file format may be incorrect."
                elif response.status_code == 401:
                    error_msg = "Authentication failed. Please check your Mapbox token has the required permissions (uploads:write, tilesets:write)"
                elif response.status_code == 422:
                    error_msg = "Invalid source data format. Ensure the GeoJSON is properly formatted."
                elif response.status_code == 413:
                    error_msg = "File too large. Try reducing the number of points in your data."
                
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error creating tileset source: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def create_tileset(self, tileset_id: str, recipe: Dict, name: str = None) -> Dict[str, Any]:
        """Create a new tileset with recipe"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            # Ensure we have a proper name
            if not name:
                name = f"Weather data {tileset_id}"
            
            data = {
                "recipe": recipe,
                "name": name,
                "description": "Weather visualization data",
                "private": False
            }
            
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.put(
                url,
                json=data,
                headers=headers
            )
            
            logger.info(f"Create tileset response: {response.status_code}")
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset: {tileset_id}")
                return {"success": True, "tileset_id": f"{self.username}.{tileset_id}"}
            else:
                error_msg = f"Failed to create tileset: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error creating tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def publish_tileset(self, tileset_id: str) -> Dict[str, Any]:
        """Publish tileset to make it available"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/publish?access_token={self.access_token}"
            
            response = requests.post(
                url,
                headers={'Content-Type': 'application/json'}
            )
            
            logger.info(f"Publish tileset response: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"Successfully published tileset: {tileset_id}")
                return {"success": True, "job_id": result.get("jobId")}
            elif response.status_code == 201:
                # Sometimes returns 201 for successful queue
                logger.info(f"Tileset publish queued: {tileset_id}")
                return {"success": True, "job_id": None}
            else:
                error_msg = f"Failed to publish tileset: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error publishing tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _convert_netcdf_to_geojson(self, netcdf_path: str) -> Optional[str]:
        """Convert NetCDF to line-delimited GeoJSON for vector tiles"""
        try:
            ds = xr.open_dataset(netcdf_path)
            
            # Find coordinates
            lons, lats = self._get_coordinates(ds)
            
            # Find wind components
            u_var, v_var = self._find_wind_components(ds)
            
            if not u_var or not v_var:
                logger.warning("No wind components found, using first two variables")
                var_list = list(ds.data_vars)
                if len(var_list) >= 2:
                    u_var = var_list[0]
                    v_var = var_list[1]
                else:
                    # If only one variable, create a dummy second one
                    if len(var_list) == 1:
                        u_var = var_list[0]
                        v_var = var_list[0]
                    else:
                        raise ValueError("Need at least 1 variable for visualization")
            
            logger.info(f"Using variables: U={u_var}, V={v_var}")
            
            # Get data
            u_data = ds[u_var]
            v_data = ds[v_var]
            
            # Handle time dimension
            if 'time' in u_data.dims:
                u_data = u_data.isel(time=0)
                v_data = v_data.isel(time=0)
            
            # Create temporary file
            temp_path = tempfile.mktemp(suffix='.ndjson')
            
            # Write features
            feature_count = 0
            with open(temp_path, 'w') as f:
                # Sample data to reduce size (max ~10000 points)
                max_points = 10000
                total_points = len(lats) * len(lons)
                
                if total_points > max_points:
                    # Calculate step size to get approximately max_points
                    step = int(np.sqrt(total_points / max_points))
                    lat_step = max(1, step)
                    lon_step = max(1, step)
                else:
                    lat_step = 1
                    lon_step = 1
                
                logger.info(f"Sampling data: {len(lats)}x{len(lons)} -> "
                          f"{len(lats)//lat_step}x{len(lons)//lon_step} points")
                
                for i in range(0, len(lats), lat_step):
                    for j in range(0, len(lons), lon_step):
                        try:
                            # Get values
                            if u_var == v_var:
                                # Single variable case
                                u_val = float(u_data.values[i, j])
                                v_val = 0.0
                            else:
                                u_val = float(u_data.values[i, j])
                                v_val = float(v_data.values[i, j])
                            
                            # Skip NaN values
                            if np.isnan(u_val):
                                u_val = 0.0
                            if np.isnan(v_val):
                                v_val = 0.0
                            
                            # Calculate derived values
                            speed = np.sqrt(u_val**2 + v_val**2)
                            direction = np.arctan2(v_val, u_val) * 180 / np.pi
                            
                            # Create feature
                            feature = {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [float(lons[j]), float(lats[i])]
                                },
                                "properties": {
                                    "u": round(u_val, 3),
                                    "v": round(v_val, 3),
                                    "speed": round(float(speed), 3),
                                    "direction": round(float(direction), 1)
                                }
                            }
                            
                            # Write as line-delimited JSON
                            f.write(json.dumps(feature, separators=(',', ':')) + '\n')
                            feature_count += 1
                            
                        except Exception as e:
                            # Skip problematic points
                            continue
            
            ds.close()
            
            logger.info(f"Created GeoJSON with {feature_count} features at {temp_path}")
            
            # Verify file is not empty
            if feature_count == 0:
                logger.error("No valid features found in NetCDF")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return None
            
            return temp_path
            
        except Exception as e:
            logger.error(f"Error converting NetCDF to GeoJSON: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def _find_wind_components(self, ds) -> Tuple[Optional[str], Optional[str]]:
        """Find U and V wind components in dataset"""
        u_patterns = ['u', 'u10', 'u_wind', 'u_component', 'eastward', 'ugrd', 'uas', 'u-wind', 'uwind']
        v_patterns = ['v', 'v10', 'v_wind', 'v_component', 'northward', 'vgrd', 'vas', 'v-wind', 'vwind']
        
        u_var = None
        v_var = None
        
        # Check all variables
        for var in ds.data_vars:
            var_lower = var.lower()
            
            # Look for U component
            if not u_var:
                for pattern in u_patterns:
                    if pattern in var_lower or var_lower == pattern:
                        u_var = var
                        logger.info(f"Found U component: {var}")
                        break
            
            # Look for V component
            if not v_var:
                for pattern in v_patterns:
                    if pattern in var_lower or var_lower == pattern:
                        v_var = var
                        logger.info(f"Found V component: {var}")
                        break
            
            # Stop if both found
            if u_var and v_var:
                break
        
        return u_var, v_var
    
    def _get_coordinates(self, ds) -> Tuple[np.ndarray, np.ndarray]:
        """Extract longitude and latitude coordinates"""
        lon_names = ['lon', 'longitude', 'x', 'X', 'long', 'LON', 'LONGITUDE']
        lat_names = ['lat', 'latitude', 'y', 'Y', 'LAT', 'LATITUDE']
        
        lons = None
        lats = None
        
        # Check coordinates
        for name in lon_names:
            if name in ds.coords:
                lons = ds.coords[name].values
                logger.info(f"Found longitude in coords: {name}")
                break
        
        for name in lat_names:
            if name in ds.coords:
                lats = ds.coords[name].values
                logger.info(f"Found latitude in coords: {name}")
                break
        
        # Check dimensions if not in coordinates
        if lons is None:
            for name in lon_names:
                if name in ds.dims:
                    # Create coordinate array from dimension
                    lons = np.arange(ds.dims[name])
                    logger.info(f"Created longitude from dimension: {name}")
                    break
        
        if lats is None:
            for name in lat_names:
                if name in ds.dims:
                    # Create coordinate array from dimension
                    lats = np.arange(ds.dims[name])
                    logger.info(f"Created latitude from dimension: {name}")
                    break
        
        # Check data variables as last resort
        if lons is None:
            for name in lon_names:
                if name in ds.data_vars:
                    lons = ds[name].values
                    logger.info(f"Found longitude in data_vars: {name}")
                    break
        
        if lats is None:
            for name in lat_names:
                if name in ds.data_vars:
                    lats = ds[name].values
                    logger.info(f"Found latitude in data_vars: {name}")
                    break
        
        if lons is None or lats is None:
            # Last resort: create synthetic coordinates
            logger.warning("Could not find coordinates, creating synthetic grid")
            if lons is None:
                lons = np.linspace(-180, 180, 100)
            if lats is None:
                lats = np.linspace(-90, 90, 50)
        
        return lons, lats
    
    def _sanitize_id(self, id_str: str) -> str:
        """Sanitize ID to meet Mapbox requirements (32 chars, alphanumeric + dash/underscore)"""
        # Convert to lowercase
        id_str = id_str.lower()
        # Replace invalid characters with underscore
        id_str = ''.join(c if c.isalnum() or c in '-_' else '_' for c in id_str)
        # Remove consecutive underscores
        while '__' in id_str:
            id_str = id_str.replace('__', '_')
        # Trim to 32 characters
        id_str = id_str[:32]
        # Remove trailing underscores
        id_str = id_str.rstrip('_')
        
        return id_str
    
    def get_tileset_status(self, tileset_id: str) -> Dict[str, Any]:
        """Get tileset information and status"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get tileset status: {response.status_code}")
                return {"error": response.text}
                
        except Exception as e:
            logger.error(f"Error getting tileset status: {str(e)}")
            return {"error": str(e)}
    
    def list_tilesets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List user's tilesets"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}?access_token={self.access_token}&limit={limit}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.error("Authentication failed when listing tilesets - check your token")
                return []
            else:
                logger.error(f"Failed to list tilesets: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing tilesets: {str(e)}")
            return []
    
    def delete_tileset(self, tileset_id: str) -> bool:
        """Delete a tileset"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            response = requests.delete(url)
            
            if response.status_code == 204:
                logger.info(f"Successfully deleted tileset: {tileset_id}")
                return True
            elif response.status_code == 404:
                logger.warning(f"Tileset not found: {tileset_id}")
                return False
            else:
                logger.error(f"Failed to delete tileset: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting tileset: {str(e)}")
            return False
    
    def get_tileset_job_status(self, tileset_id: str, job_id: str) -> Dict[str, Any]:
        """Check the status of a tileset processing job"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/jobs/{job_id}?access_token={self.access_token}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": f"Failed to get job status: {response.status_code}"}
                
        except Exception as e:
            return {"error": str(e)}
    
    def wait_for_processing(self, tileset_id: str, job_id: str, timeout: int = 300) -> Dict[str, Any]:
        """Wait for tileset processing to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                status = self.get_tileset_job_status(tileset_id, job_id)
                
                if 'error' not in status:
                    if status.get('stage') == 'success':
                        logger.info(f"Tileset processing completed successfully")
                        return {"success": True, "status": status}
                    elif status.get('stage') == 'failed':
                        logger.error(f"Tileset processing failed: {status}")
                        return {"success": False, "error": "Processing failed", "status": status}
                    else:
                        logger.info(f"Processing status: {status.get('stage', 'unknown')}")
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error checking job status: {e}")
                
        return {"success": False, "error": "Processing timeout"}