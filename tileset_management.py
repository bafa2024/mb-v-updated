"""
Mapbox Tileset Management Module - Fixed Version with Enhanced Format Detection
Handles creation and management of Mapbox tilesets from NetCDF data
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
        
    def process_netcdf_to_tileset(self, netcdf_path: str, tileset_id: str, recipe: Dict = None) -> Dict[str, Any]:
        """Process NetCDF to vector tileset with proper error handling"""
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
            try:
                if os.path.exists(geojson_path):
                    os.remove(geojson_path)
            except Exception as e:
                logger.warning(f"Could not remove temp file: {e}")
            
            if not source_result["success"]:
                return source_result
            
            # Step 3: Create tileset with recipe
            tileset_id = self._sanitize_id(tileset_id)
            
            if not recipe:
                recipe = {
                    "version": 1,
                    "layers": {
                        "weather_data": {
                            "source": f"mapbox://tileset-source/{self.username}/{source_id}",
                            "minzoom": 0,
                            "maxzoom": 10
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
            
            # Save source layer info
            source_layer = "weather_data"
            
            return {
                "success": True,
                "tileset_id": f"{self.username}.{tileset_id}",
                "job_id": publish_result.get("job_id"),
                "format": "vector",
                "source_layer": source_layer,
                "recipe_id": tileset_result.get("recipe_id"),
                "publish_job_id": publish_result.get("job_id")
            }
            
        except Exception as e:
            logger.error(f"Error processing NetCDF: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def create_tileset_source(self, source_id: str, file_path: str) -> Dict[str, Any]:
        """Upload source data to Mapbox - FIXED VERSION"""
        try:
            # First, delete any existing source with the same ID
            delete_url = f"{self.api_base}/tilesets/v1/sources/{self.username}/{source_id}?access_token={self.access_token}"
            delete_response = requests.delete(delete_url)
            if delete_response.status_code == 204:
                logger.info(f"Deleted existing source: {source_id}")
            
            # Upload new source
            url = f"{self.api_base}/tilesets/v1/sources/{self.username}/{source_id}?access_token={self.access_token}"
            
            # Read the entire file content
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            # The Mapbox API expects the file to be sent as multipart/form-data
            files = {
                'file': (f'{source_id}.json', file_content, 'application/x-ndjson')
            }
            
            logger.info(f"Uploading source: {source_id} ({len(file_content)} bytes)")
            
            response = requests.post(url, files=files)
            
            logger.info(f"Source upload response: {response.status_code}")
            if response.text:
                logger.info(f"Response body: {response.text}")
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset source: {source_id}")
                return {"success": True, "source_id": f"{self.username}.{source_id}"}
            else:
                error_msg = f"Failed to create source: {response.status_code}"
                if response.text:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', error_msg)
                    except:
                        error_msg += f" - {response.text}"
                
                logger.error(error_msg)
                
                # Provide helpful error messages
                if response.status_code == 401:
                    error_msg = "Authentication failed. Check your Mapbox token permissions (needs uploads:write)"
                elif response.status_code == 422:
                    error_msg = "Invalid source data format. Check the GeoJSON structure"
                elif response.status_code == 413:
                    error_msg = "File too large. Try reducing the number of data points"
                
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error creating tileset source: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def create_tileset(self, tileset_id: str, recipe: Dict, name: str = None) -> Dict[str, Any]:
        """Create a new tileset with recipe"""
        try:
            # First, try to delete any existing tileset
            delete_url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            delete_response = requests.delete(delete_url)
            if delete_response.status_code == 204:
                logger.info(f"Deleted existing tileset: {tileset_id}")
                # Wait a moment for deletion to process
                time.sleep(2)
            
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            if not name:
                name = f"Weather data {tileset_id}"
            
            data = {
                "recipe": recipe,
                "name": name,
                "description": "Weather visualization data created from NetCDF",
                "private": False
            }
            
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                url,
                json=data,
                headers=headers
            )
            
            logger.info(f"Create tileset response: {response.status_code}")
            if response.text:
                logger.info(f"Response body: {response.text}")
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset: {tileset_id}")
                result = response.json() if response.text else {}
                return {
                    "success": True, 
                    "tileset_id": f"{self.username}.{tileset_id}",
                    "recipe_id": result.get("id")
                }
            else:
                error_msg = f"Failed to create tileset: {response.status_code}"
                if response.text:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', error_msg)
                    except:
                        error_msg += f" - {response.text}"
                        
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error creating tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def publish_tileset(self, tileset_id: str) -> Dict[str, Any]:
        """Publish tileset to make it available"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/publish?access_token={self.access_token}"
            
            response = requests.post(url)
            
            logger.info(f"Publish tileset response: {response.status_code}")
            if response.text:
                logger.info(f"Response body: {response.text}")
            
            if response.status_code in [200, 201, 202]:
                result = response.json() if response.text else {}
                job_id = result.get("jobId") or result.get("id")
                logger.info(f"Successfully published tileset: {tileset_id}, job_id: {job_id}")
                return {"success": True, "job_id": job_id}
            else:
                error_msg = f"Failed to publish tileset: {response.status_code}"
                if response.text:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', error_msg)
                    except:
                        error_msg += f" - {response.text}"
                        
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error publishing tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def check_tileset_format(self, tileset_id: str) -> Dict[str, Any]:
        """
        Check the actual format of a tileset on Mapbox
        Returns detailed information about the tileset type
        """
        try:
            # Ensure we have the full tileset ID
            if '.' not in tileset_id:
                tileset_id = f"{self.username}.{tileset_id}"
            
            url = f"{self.api_base}/tilesets/v1/{tileset_id}?access_token={self.access_token}"
            response = requests.get(url)
            
            if response.status_code == 200:
                tileset_info = response.json()
                
                # Determine the actual format
                tileset_type = tileset_info.get('type', '').lower()
                format_type = 'vector'  # default
                
                # Check various indicators for raster format
                if any(indicator in tileset_type for indicator in ['raster', 'rasterarray', 'raster-array']):
                    format_type = 'raster-array'
                elif 'vector' in tileset_type:
                    format_type = 'vector'
                
                # Additional checks
                layers = tileset_info.get('layers', [])
                if layers:
                    # Check layer types
                    for layer in layers:
                        if isinstance(layer, dict):
                            layer_type = layer.get('type', '').lower()
                            if 'raster' in layer_type:
                                format_type = 'raster-array'
                                break
                
                return {
                    'success': True,
                    'tileset_id': tileset_id,
                    'format': format_type,
                    'type': tileset_type,
                    'layers': layers,
                    'name': tileset_info.get('name', ''),
                    'created': tileset_info.get('created', ''),
                    'modified': tileset_info.get('modified', '')
                }
            elif response.status_code == 404:
                return {
                    'success': False,
                    'error': 'Tileset not found',
                    'code': 404
                }
            else:
                return {
                    'success': False,
                    'error': f'Failed to get tileset info: {response.status_code}',
                    'code': response.status_code
                }
                
        except Exception as e:
            logger.error(f"Error checking tileset format: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def verify_tileset_ready(self, tileset_id: str, max_attempts: int = 30) -> Dict[str, Any]:
        """
        Verify that a tileset is ready for use
        Waits for processing to complete if needed
        """
        try:
            if '.' not in tileset_id:
                tileset_id = f"{self.username}.{tileset_id}"
            
            for attempt in range(max_attempts):
                # Check tileset status
                format_check = self.check_tileset_format(tileset_id)
                
                if format_check.get('success'):
                    # Tileset exists, check if it's ready
                    tileset_info = format_check
                    
                    # For raster tilesets, we might need to wait for processing
                    if format_check.get('format') == 'raster-array':
                        # Check if there are any active jobs
                        jobs_url = f"{self.api_base}/tilesets/v1/{tileset_id}/jobs?access_token={self.access_token}&limit=1"
                        jobs_response = requests.get(jobs_url)
                        
                        if jobs_response.status_code == 200:
                            jobs = jobs_response.json()
                            if jobs and len(jobs) > 0:
                                latest_job = jobs[0]
                                if latest_job.get('stage') not in ['success', 'failed']:
                                    logger.info(f"Tileset still processing, attempt {attempt + 1}/{max_attempts}")
                                    time.sleep(5)
                                    continue
                    
                    # Tileset is ready
                    return {
                        'success': True,
                        'ready': True,
                        'format': format_check.get('format'),
                        'tileset_info': tileset_info
                    }
                elif format_check.get('code') == 404:
                    # Tileset doesn't exist yet
                    logger.info(f"Tileset not found yet, attempt {attempt + 1}/{max_attempts}")
                    time.sleep(5)
                    continue
                else:
                    # Some other error
                    return format_check
            
            return {
                'success': False,
                'error': 'Tileset verification timeout',
                'ready': False
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'ready': False
            }
    
    def _convert_netcdf_to_geojson(self, netcdf_path: str) -> Optional[str]:
        """Convert NetCDF to line-delimited GeoJSON for vector tiles"""
        try:
            ds = xr.open_dataset(netcdf_path)
            
            # Find coordinates
            lons, lats = self._get_coordinates(ds)
            
            # Find wind components or use first two variables
            u_var, v_var = self._find_wind_components(ds)
            
            if not u_var or not v_var:
                logger.warning("No wind components found, using first two variables")
                var_list = list(ds.data_vars)
                if len(var_list) >= 2:
                    u_var = var_list[0]
                    v_var = var_list[1]
                elif len(var_list) == 1:
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
            
            # Create temporary file for NDJSON
            temp_fd, temp_path = tempfile.mkstemp(suffix='.json')
            os.close(temp_fd)  # Close the file descriptor
            
            # Write features
            feature_count = 0
            with open(temp_path, 'w', encoding='utf-8') as f:
                # Sample data to reduce size (max ~5000 points for better performance)
                max_points = 5000
                total_points = len(lats) * len(lons)
                
                if total_points > max_points:
                    # Calculate step size
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
                            u_val = float(u_data.values[i, j])
                            v_val = float(v_data.values[i, j])
                            
                            # Skip NaN values
                            if np.isnan(u_val):
                                u_val = 0.0
                            if np.isnan(v_val):
                                v_val = 0.0
                            
                            # Calculate derived values
                            speed = float(np.sqrt(u_val**2 + v_val**2))
                            direction = float(np.arctan2(v_val, u_val) * 180 / np.pi)
                            
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
                                    "speed": round(speed, 3),
                                    "direction": round(direction, 1)
                                }
                            }
                            
                            # Write as line-delimited JSON
                            json_line = json.dumps(feature, separators=(',', ':'))
                            f.write(json_line + '\n')
                            feature_count += 1
                            
                        except Exception as e:
                            logger.warning(f"Skipping point ({i},{j}): {e}")
                            continue
            
            ds.close()
            
            logger.info(f"Created GeoJSON with {feature_count} features at {temp_path}")
            
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
        
        for var in ds.data_vars:
            var_lower = var.lower()
            
            if not u_var:
                for pattern in u_patterns:
                    if pattern in var_lower or var_lower == pattern:
                        u_var = var
                        logger.info(f"Found U component: {var}")
                        break
            
            if not v_var:
                for pattern in v_patterns:
                    if pattern in var_lower or var_lower == pattern:
                        v_var = var
                        logger.info(f"Found V component: {var}")
                        break
            
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
                    lons = np.arange(ds.dims[name])
                    logger.warning(f"Created synthetic longitude from dimension: {name}")
                    break
        
        if lats is None:
            for name in lat_names:
                if name in ds.dims:
                    lats = np.arange(ds.dims[name])
                    logger.warning(f"Created synthetic latitude from dimension: {name}")
                    break
        
        if lons is None or lats is None:
            raise ValueError("Could not find longitude/latitude coordinates")
        
        # Ensure proper shape
        if lons.ndim > 1:
            lons = lons[0, :]
        if lats.ndim > 1:
            lats = lats[:, 0]
            
        return lons, lats
    
    def _sanitize_id(self, id_str: str) -> str:
        """Sanitize ID to meet Mapbox requirements"""
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
            # Try with full tileset ID first
            if '.' not in tileset_id:
                tileset_id = f"{self.username}.{tileset_id}"
                
            url = f"{self.api_base}/tilesets/v1/{tileset_id}?access_token={self.access_token}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"error": "Tileset not found"}
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
                logger.error("Authentication failed - check your token")
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