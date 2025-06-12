# mts_raster_manager.py
"""
Mapbox Tiling Service (MTS) Raster Manager
Handles creation of raster-array tilesets using MTS for particle animation
Works with free tier Mapbox accounts!
"""

import os
import json
import logging
import tempfile
import time
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
import requests
import xarray as xr
import numpy as np

logger = logging.getLogger(__name__)


class MTSRasterManager:
    """Manages raster-array tileset creation using Mapbox Tiling Service"""
    
    def __init__(self, access_token: str, username: str):
        self.access_token = access_token
        self.username = username
        self.api_base = "https://api.mapbox.com"
        
    async def create_raster_tileset(self, netcdf_path: str, tileset_id: str) -> Dict[str, Any]:
        """
        Create a raster-array tileset using MTS from NetCDF data
        This works with free tier accounts!
        """
        logger.info(f"Creating MTS raster tileset from {netcdf_path}")
        
        try:
            # Step 1: Analyze NetCDF structure
            ds = xr.open_dataset(netcdf_path)
            wind_components = self._find_wind_components(ds)
            
            if not wind_components:
                logger.error("No wind components found in NetCDF")
                return {
                    'success': False,
                    'error': 'No wind components (u/v) found in NetCDF file'
                }
            
            logger.info(f"Found wind components: U={wind_components['u']}, V={wind_components['v']}")
            
            # Step 2: Create tileset source
            source_id = f"{tileset_id}_src"
            source_result = await self._create_tileset_source(netcdf_path, source_id)
            
            if not source_result['success']:
                return source_result
            
            # Step 3: Create MTS recipe for raster-array
            recipe = self._create_raster_recipe(wind_components, source_id)
            
            # Step 4: Create tileset with recipe
            tileset_result = await self._create_tileset(tileset_id, recipe)
            
            if not tileset_result['success']:
                return tileset_result
            
            # Step 5: Publish tileset
            publish_result = await self._publish_tileset(tileset_id)
            
            if not publish_result['success']:
                return publish_result
            
            ds.close()
            
            return {
                'success': True,
                'tileset_id': f"{self.username}.{tileset_id}",
                'format': 'raster-array',
                'source_layer': '10winds',  # Standard layer name for wind data
                'recipe_id': tileset_result.get('recipe_id'),
                'publish_job_id': publish_result.get('job_id')
            }
            
        except Exception as e:
            logger.error(f"Error creating MTS raster tileset: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }
    
    def _find_wind_components(self, ds) -> Optional[Dict[str, str]]:
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
        
        if u_var and v_var:
            return {"u": u_var, "v": v_var}
        
        return None
    
    async def _create_tileset_source(self, netcdf_path: str, source_id: str) -> Dict[str, Any]:
        """Upload NetCDF file as tileset source"""
        try:
            url = f"{self.api_base}/tilesets/v1/sources/{self.username}/{source_id}?access_token={self.access_token}"
            
            with open(netcdf_path, 'rb') as f:
                files = {'file': (os.path.basename(netcdf_path), f, 'application/x-netcdf')}
                response = requests.post(url, files=files)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset source: {source_id}")
                return {
                    'success': True,
                    'source_id': f"{self.username}.{source_id}"
                }
            else:
                error_msg = f"Failed to create source: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"Error uploading source: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _create_raster_recipe(self, wind_components: Dict[str, str], source_id: str) -> Dict[str, Any]:
        """
        Create MTS recipe for raster-array wind data
        This recipe structure is based on Mapbox documentation for wind visualization
        """
        recipe = {
            "version": 1,
            "sources": [
                {
                    "uri": f"mapbox://tileset-source/{self.username}/{source_id}"
                }
            ],
            "layers": {
                "10winds": {  # Standard layer name for 10m wind data
                    "source_rules": [
                        {
                            "select": [
                                wind_components["u"],  # U component must be first
                                wind_components["v"]   # V component must be second
                            ]
                        }
                    ],
                    "tilesize": 512,
                    "buffer": 1,
                    "encoding": {
                        "scale": [0.01, 0.01],  # Scale factor for wind data
                        "offset": [0, 0]
                    }
                }
            },
            "type": "rasterarray",  # Specify rasterarray type
            "minzoom": 0,
            "maxzoom": 6
        }
        
        return recipe
    
    async def _create_tileset(self, tileset_id: str, recipe: Dict[str, Any]) -> Dict[str, Any]:
        """Create tileset with MTS recipe"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            
            # First, try to delete any existing tileset with same ID
            requests.delete(url)
            
            # Create new tileset
            headers = {'Content-Type': 'application/json'}
            data = {
                "recipe": recipe,
                "name": f"Wind Animation {tileset_id}",
                "description": "Raster-array tileset for wind particle animation",
                "private": False
            }
            
            response = requests.put(url, json=data, headers=headers)
            
            if response.status_code in [200, 201]:
                logger.info(f"Successfully created tileset: {tileset_id}")
                result = response.json() if response.text else {}
                return {
                    'success': True,
                    'tileset_id': f"{self.username}.{tileset_id}",
                    'recipe_id': result.get('id')
                }
            else:
                error_msg = f"Failed to create tileset: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"Error creating tileset: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _publish_tileset(self, tileset_id: str) -> Dict[str, Any]:
        """Publish the tileset to make it available"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/publish?access_token={self.access_token}"
            
            response = requests.post(url, headers={'Content-Type': 'application/json'})
            
            if response.status_code in [200, 201, 202]:
                result = response.json() if response.text else {}
                job_id = result.get('jobId') or result.get('id')
                logger.info(f"Successfully initiated tileset publish: {tileset_id}, job_id: {job_id}")
                
                # Wait for publishing to complete
                if job_id:
                    await self._wait_for_publish(tileset_id, job_id)
                
                return {
                    'success': True,
                    'job_id': job_id
                }
            else:
                error_msg = f"Failed to publish tileset: {response.status_code} - {response.text}"
                logger.error(error_msg)
                return {
                    'success': False,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"Error publishing tileset: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _wait_for_publish(self, tileset_id: str, job_id: str, timeout: int = 300):
        """Wait for tileset publishing to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}/jobs/{job_id}?access_token={self.access_token}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    job_status = response.json()
                    stage = job_status.get('stage', 'unknown')
                    
                    logger.info(f"Publish job {job_id} status: {stage}")
                    
                    if stage == 'success':
                        logger.info(f"Tileset published successfully: {tileset_id}")
                        return True
                    elif stage == 'failed':
                        logger.error(f"Tileset publish failed: {job_status}")
                        return False
                    
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error checking publish status: {e}")
                
        logger.warning(f"Publish job timeout for tileset: {tileset_id}")
        return False
    
    def get_tileset_status(self, tileset_id: str) -> Dict[str, Any]:
        """Get tileset information and status"""
        try:
            url = f"{self.api_base}/tilesets/v1/{self.username}.{tileset_id}?access_token={self.access_token}"
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {"error": f"Failed to get tileset status: {response.status_code}"}
                
        except Exception as e:
            return {"error": str(e)}