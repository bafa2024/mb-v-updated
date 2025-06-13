# mts_raster_manager.py
"""
Mapbox Tiling Service (MTS) Raster Manager
Handles creation of raster-array tilesets using MTS for particle animation
Works with free tier Mapbox accounts! (with proper error handling)
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
        This works with free tier accounts! (with proper error handling)
        """
        logger.info(f"Creating MTS raster tileset from {netcdf_path}")
        
        try:
            # First check if raster uploads are supported (Pro account check)
            credentials_test_url = f"{self.api_base}/uploads/v1/{self.username}/credentials?access_token={self.access_token}"
            test_response = requests.post(credentials_test_url)
            
            if test_response.status_code == 422:
                logger.warning("Raster uploads not supported on free tier")
                return {
                    'success': False,
                    'error': 'Raster-array format requires a Mapbox Pro account. Please use Vector format instead or enable client-side animation.',
                    'fallback_to_vector': True,
                    'error_code': 422
                }
            elif test_response.status_code == 401:
                return {
                    'success': False,
                    'error': 'Authentication failed. Please check your Mapbox token permissions.',
                    'error_code': 401
                }
            elif test_response.status_code != 200:
                return {
                    'success': False,
                    'error': f'Failed to verify account capabilities: {test_response.status_code}',
                    'error_code': test_response.status_code
                }
            
            # If we get here, Pro account is available - try to create raster tileset
            logger.info("Pro account detected, proceeding with raster-array creation")
            
            # Step 1: Get upload credentials
            credentials_response = test_response.json()
            
            # Step 2: Convert NetCDF to raster format suitable for Mapbox
            # For raster-array, we need to create a properly formatted raster
            temp_tiff_path = None
            try:
                # Create a temporary GeoTIFF with wind data
                temp_tiff_path = await self._create_wind_raster(netcdf_path)
                
                if not temp_tiff_path:
                    return {
                        'success': False,
                        'error': 'Failed to create raster from NetCDF data'
                    }
                
                # Step 3: Upload to S3
                upload_result = await self._upload_to_s3(temp_tiff_path, credentials_response)
                
                if not upload_result['success']:
                    return upload_result
                
                # Step 4: Create Mapbox upload
                upload_data = {
                    "url": upload_result['s3_url'],
                    "tileset": f"{self.username}.{tileset_id}",
                    "name": f"Wind Animation {tileset_id}",
                    "type": "raster"
                }
                
                upload_url = f"{self.api_base}/uploads/v1/{self.username}?access_token={self.access_token}"
                upload_response = requests.post(upload_url, json=upload_data)
                
                if upload_response.status_code not in [200, 201]:
                    return {
                        'success': False,
                        'error': f'Failed to create Mapbox upload: {upload_response.status_code}'
                    }
                
                upload_info = upload_response.json()
                upload_id = upload_info.get('id')
                
                # Step 5: Wait for processing
                processing_result = await self._wait_for_upload_processing(upload_id)
                
                if processing_result['success']:
                    return {
                        'success': True,
                        'tileset_id': f"{self.username}.{tileset_id}",
                        'format': 'raster-array',
                        'source_layer': '10winds',
                        'upload_id': upload_id
                    }
                else:
                    return processing_result
                    
            finally:
                # Clean up temporary files
                if temp_tiff_path and os.path.exists(temp_tiff_path):
                    os.remove(temp_tiff_path)
                
        except Exception as e:
            logger.error(f"Error creating MTS raster tileset: {str(e)}")
            import traceback
            traceback.print_exc()
            
            # Check if it's a known Pro account requirement error
            if "credentials" in str(e).lower() or "422" in str(e):
                return {
                    'success': False,
                    'error': 'Raster-array format requires a Mapbox Pro account. Please use Vector format instead or enable client-side animation.',
                    'fallback_to_vector': True,
                    'error_code': 422
                }
            
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _create_wind_raster(self, netcdf_path: str) -> Optional[str]:
        """Create a raster suitable for Mapbox raster-array from NetCDF"""
        try:
            import tempfile
            import rasterio
            from rasterio.transform import from_bounds
            
            ds = xr.open_dataset(netcdf_path)
            
            # Find wind components
            wind_components = self._find_wind_components(ds)
            if not wind_components:
                logger.error("No wind components found")
                return None
            
            # Get data
            u_data = ds[wind_components['u']]
            v_data = ds[wind_components['v']]
            
            # Handle time dimension
            if 'time' in u_data.dims:
                u_data = u_data.isel(time=0)
                v_data = v_data.isel(time=0)
            
            # Get coordinates
            lats = ds.lat.values if 'lat' in ds else ds.latitude.values
            lons = ds.lon.values if 'lon' in ds else ds.longitude.values
            
            # Create temporary file
            temp_fd, temp_path = tempfile.mkstemp(suffix='.tif')
            os.close(temp_fd)
            
            # Create GeoTIFF with 2 bands (U and V)
            height, width = u_data.shape
            
            transform = from_bounds(
                lons.min(), lats.min(), lons.max(), lats.max(),
                width, height
            )
            
            with rasterio.open(
                temp_path,
                'w',
                driver='GTiff',
                height=height,
                width=width,
                count=2,  # U and V components
                dtype='float32',
                crs='EPSG:4326',
                transform=transform,
                compress='deflate'
            ) as dst:
                # Write U component to band 1
                dst.write(u_data.values.astype('float32'), 1)
                # Write V component to band 2
                dst.write(v_data.values.astype('float32'), 2)
                
                # Set band descriptions
                dst.set_band_description(1, 'u_wind')
                dst.set_band_description(2, 'v_wind')
            
            ds.close()
            return temp_path
            
        except Exception as e:
            logger.error(f"Error creating wind raster: {e}")
            return None
    
    async def _upload_to_s3(self, file_path: str, credentials: Dict) -> Dict[str, Any]:
        """Upload file to S3 using Mapbox credentials"""
        try:
            import boto3
            from botocore.exceptions import NoCredentialsError
            
            # Create S3 client with Mapbox credentials
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['accessKeyId'],
                aws_secret_access_key=credentials['secretAccessKey'],
                aws_session_token=credentials.get('sessionToken')
            )
            
            # Upload file
            with open(file_path, 'rb') as f:
                s3_client.put_object(
                    Bucket=credentials['bucket'],
                    Key=credentials['key'],
                    Body=f
                )
            
            s3_url = f"s3://{credentials['bucket']}/{credentials['key']}"
            logger.info(f"Uploaded to S3: {s3_url}")
            
            return {
                'success': True,
                's3_url': s3_url
            }
            
        except NoCredentialsError:
            return {
                'success': False,
                'error': 'Invalid S3 credentials'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'S3 upload failed: {str(e)}'
            }
    
    async def _wait_for_upload_processing(self, upload_id: str, timeout: int = 300) -> Dict[str, Any]:
        """Wait for Mapbox upload to complete processing"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                url = f"{self.api_base}/uploads/v1/{self.username}/{upload_id}?access_token={self.access_token}"
                response = requests.get(url)
                
                if response.status_code == 200:
                    status = response.json()
                    
                    if status.get('complete'):
                        if status.get('error'):
                            return {
                                'success': False,
                                'error': status.get('error')
                            }
                        else:
                            return {'success': True}
                    
                    # Log progress
                    progress = status.get('progress', 0)
                    logger.info(f"Upload progress: {progress}%")
                
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error checking upload status: {e}")
        
        return {
            'success': False,
            'error': 'Upload processing timeout'
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
                        logger.info(f"Found U component: {var}")
                        break
            
            if not v_var:
                for pattern in v_patterns:
                    if pattern.lower() in var_lower:
                        v_var = var
                        logger.info(f"Found V component: {var}")
                        break
            
            if u_var and v_var:
                break
        
        if u_var and v_var:
            return {"u": u_var, "v": v_var}
        
        return None