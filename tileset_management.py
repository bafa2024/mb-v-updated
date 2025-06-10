"""
Mapbox Tileset Management Module
Handles creation and management of Mapbox tilesets from NetCDF data
"""

import os
import json
import logging
import tempfile
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


class MapboxTilesetManager:
    """Manages Mapbox tileset operations"""
    
    def __init__(self, access_token: str, username: str):
        self.access_token = access_token
        self.username = username
        self.api_base = "https://api.mapbox.com"
        self.headers = {
            "Content-Type": "application/json",
        }
        
    def create_tileset_source(self, source_id: str, file_path: str) -> Dict[str, Any]:
        """Upload source data to Mapbox"""
        try:
            # Sanitize source_id to meet Mapbox requirements
            # Must be 32 chars or less, only a-z, 0-9, -, _
            source_id = source_id.lower()
            source_id = ''.join(c if c.isalnum() or c in '-_' else '_' for c in source_id)
            source_id = source_id[:32]  # Truncate to 32 characters
            
            logger.info(f"Sanitized source_id: {source_id}")
            
            # First, create the source
            create_url = f"{self.api_base}/tilesets/v1/sources/{self.username}/{source_id}?access_token={self.access_token}"
            
            # Upload the file with correct content type for line-delimited JSON
            with open(file_path, 'rb') as f:
                files = {'file': (Path(file_path).name, f, 'application/x-ndjson')}
                response = requests.post(create_url, files=files)
                
            if response.status_code == 200:
                logger.info(f"Successfully created tileset source: {source_id}")
                return {"success": True, "source_id": f"{self.username}.{source_id}"}
            else:
                logger.error(f"Failed to create source: {response.status_code} - {response.text}")
                return {"success": False, "error": response.text}
                
        except Exception as e:
            logger.error(f"Error creating tileset source: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def create_tileset(self, tileset_id: str, recipe: Dict, name: str = None) -> Dict[str, Any]:
        """Create a new tileset with recipe"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            data = {
                "recipe": recipe,
                "name": name or tileset_id
            }
            
            response = requests.post(url, json=data, headers=self.headers)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset: {tileset_id}")
                return {"success": True, "tileset_id": f"{self.username}.{tileset_id}"}
            else:
                logger.error(f"Failed to create tileset: {response.status_code} - {response.text}")
                return {"success": False, "error": response.text}
                
        except Exception as e:
            logger.error(f"Error creating tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def publish_tileset(self, tileset_id: str) -> Dict[str, Any]:
        """Publish tileset to make it available"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/publish?access_token={self.access_token}"
            
            response = requests.post(url, headers=self.headers)
            
            if response.status_code == 200:
                logger.info(f"Successfully published tileset: {tileset_id}")
                return {"success": True, "job_id": response.json().get("jobId")}
            else:
                logger.error(f"Failed to publish tileset: {response.status_code} - {response.text}")
                return {"success": False, "error": response.text}
                
        except Exception as e:
            logger.error(f"Error publishing tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def get_tileset_status(self, tileset_id: str) -> Dict[str, Any]:
        """Get tileset information and status"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            response = requests.get(url, headers=self.headers)
            
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
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
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
            
            response = requests.delete(url, headers=self.headers)
            
            if response.status_code == 204:
                logger.info(f"Successfully deleted tileset: {tileset_id}")
                return True
            else:
                logger.error(f"Failed to delete tileset: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting tileset: {str(e)}")
            return False
    
    def process_netcdf_to_tileset(self, netcdf_path: str, tileset_id: str, recipe: Dict) -> Dict[str, Any]:
        """Complete process to convert NetCDF to Mapbox tileset"""
        try:
            # Step 1: Convert NetCDF to GeoJSON (you'll need to implement this based on your data)
            geojson_path = self._convert_netcdf_to_geojson(netcdf_path)
            
            if not geojson_path:
                return {"success": False, "error": "Failed to convert NetCDF to GeoJSON"}
            
            # Step 2: Create tileset source with sanitized ID
            # Source ID must be different from tileset ID and also sanitized
            source_id = f"{tileset_id}_src"
            source_id = source_id.lower()
            source_id = ''.join(c if c.isalnum() or c in '-_' else '_' for c in source_id)
            source_id = source_id[:32]  # Ensure it's no more than 32 chars
            
            source_result = self.create_tileset_source(source_id, geojson_path)
            
            if not source_result["success"]:
                return source_result
            
            # Step 3: Create a simple recipe that Mapbox will accept
            # Use a minimal recipe structure
            simple_recipe = {
                "version": 1,
                "layers": {
                    "data": {
                        "source": f"mapbox://tileset-source/{self.username}/{source_id}",
                        "minzoom": 0,
                        "maxzoom": 5
                    }
                }
            }
            
            # Step 4: Create tileset (also ensure tileset_id is sanitized)
            tileset_id = tileset_id.lower()
            tileset_id = ''.join(c if c.isalnum() or c in '-_' else '_' for c in tileset_id)
            tileset_id = tileset_id[:32]
            
            tileset_result = self.create_tileset(tileset_id, simple_recipe)
            
            if not tileset_result["success"]:
                return tileset_result
            
            # Step 5: Publish tileset
            publish_result = self.publish_tileset(tileset_id)
            
            if not publish_result["success"]:
                return publish_result
            
            # Clean up temporary file
            if os.path.exists(geojson_path):
                os.remove(geojson_path)
            
            return {
                "success": True,
                "tileset_id": f"{self.username}.{tileset_id}",
                "job_id": publish_result.get("job_id")
            }
            
        except Exception as e:
            logger.error(f"Error processing NetCDF to tileset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def _convert_netcdf_to_geojson(self, netcdf_path: str) -> Optional[str]:
        """Convert NetCDF to GeoJSON format (line-delimited for Mapbox)"""
        try:
            import xarray as xr
            import numpy as np
            
            # Open NetCDF file
            ds = xr.open_dataset(netcdf_path)
            
            # Extract coordinates
            if 'lon' in ds.coords:
                lons = ds.coords['lon'].values
            elif 'longitude' in ds.coords:
                lons = ds.coords['longitude'].values
            else:
                raise ValueError("No longitude coordinate found")
            
            if 'lat' in ds.coords:
                lats = ds.coords['lat'].values
            elif 'latitude' in ds.coords:
                lats = ds.coords['latitude'].values
            else:
                raise ValueError("No latitude coordinate found")
            
            # Create a temporary file for line-delimited GeoJSON
            temp_path = tempfile.mktemp(suffix='.geojson')
            
            with open(temp_path, 'w') as f:
                # Sample the data to avoid too many points
                lat_step = max(1, len(lats) // 50)
                lon_step = max(1, len(lons) // 50)
                
                # Get first variable for demo
                var_names = list(ds.data_vars)
                if var_names:
                    var_name = var_names[0]
                    data = ds[var_name]
                    
                    # Handle time dimension if present
                    if 'time' in data.dims:
                        data = data.isel(time=0)
                    
                    # Write features as line-delimited JSON (NOT FeatureCollection)
                    for i in range(0, len(lats), lat_step):
                        for j in range(0, len(lons), lon_step):
                            value = float(data.values[i, j])
                            if not np.isnan(value):
                                feature = {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [float(lons[j]), float(lats[i])]
                                    },
                                    "properties": {
                                        var_name: value,
                                        "lat": float(lats[i]),
                                        "lon": float(lons[j])
                                    }
                                }
                                # Write each feature on its own line
                                f.write(json.dumps(feature) + '\n')
            
            ds.close()
            logger.info(f"Created line-delimited GeoJSON: {temp_path}")
            return temp_path
            
        except Exception as e:
            logger.error(f"Error converting NetCDF to GeoJSON: {str(e)}")
            return None
    
    def check_job_status(self, tileset_id: str, job_id: str) -> Dict[str, Any]:
        """Check the status of a tileset processing job"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/jobs/{job_id}?access_token={self.access_token}"
            
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to check job status: {response.status_code}")
                return {"error": response.text}
                
        except Exception as e:
            logger.error(f"Error checking job status: {str(e)}")
            return {"error": str(e)}