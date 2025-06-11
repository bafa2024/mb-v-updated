# tileset_management_enhanced.py
"""
Enhanced Mapbox Tileset Management with proper raster-array support
"""

import os
import json
import logging
import tempfile
import time
from pathlib import Path
from typing import Dict, Any, Optional
import requests
import boto3
from botocore.exceptions import NoCredentialsError

# Import our raster converter
from utils.raster_array_converter import RasterArrayConverter, create_raster_array_recipe

logger = logging.getLogger(__name__)


class EnhancedMapboxTilesetManager:
    """Enhanced manager with proper raster-array upload support"""
    
    def __init__(self, access_token: str, username: str):
        self.access_token = access_token
        self.username = username
        self.api_base = "https://api.mapbox.com"
        
    def create_raster_array_tileset(self, netcdf_path: str, tileset_id: str) -> Dict[str, Any]:
        """
        Create a raster-array tileset for wind particle animation
        This requires Mapbox upload API access
        """
        logger.info(f"Creating raster-array tileset from {netcdf_path}")
        
        try:
            # Step 1: Convert NetCDF to raster array GeoTIFF
            temp_tiff = tempfile.mktemp(suffix='.tif')
            result = RasterArrayConverter.netcdf_to_raster_array(netcdf_path, temp_tiff)
            
            if not result['success']:
                return {
                    'success': False,
                    'error': f"Failed to convert NetCDF: {result['error']}"
                }
            
            # Step 2: Convert to Cloud Optimized GeoTIFF
            cog_path = tempfile.mktemp(suffix='_cog.tif')
            if not RasterArrayConverter.create_cog(temp_tiff, cog_path):
                return {
                    'success': False,
                    'error': 'Failed to create Cloud Optimized GeoTIFF'
                }
            
            # Clean up temp file
            if os.path.exists(temp_tiff):
                os.remove(temp_tiff)
            
            # Step 3: Upload to Mapbox
            upload_result = self._upload_raster_to_mapbox(cog_path, tileset_id)
            
            # Clean up COG file
            if os.path.exists(cog_path):
                os.remove(cog_path)
            
            return upload_result
            
        except Exception as e:
            logger.error(f"Error creating raster-array tileset: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }
    
    def _upload_raster_to_mapbox(self, geotiff_path: str, tileset_id: str) -> Dict[str, Any]:
        """Upload raster data using Mapbox Uploads API"""
        try:
            # Step 1: Request upload credentials
            logger.info("Requesting upload credentials from Mapbox...")
            
            credentials_url = f"{self.api_base}/uploads/v1/{self.username}/credentials?access_token={self.access_token}"
            cred_response = requests.post(credentials_url)
            
            if cred_response.status_code != 200:
                error_msg = f"Failed to get upload credentials: {cred_response.status_code} - {cred_response.text}"
                logger.error(error_msg)
                
                if cred_response.status_code == 401:
                    return {
                        'success': False,
                        'error': 'Authentication failed. Ensure your token has uploads:write scope'
                    }
                elif cred_response.status_code == 422:
                    return {
                        'success': False,
                        'error': 'Account does not support raster uploads. Pro account required.'
                    }
                
                return {'success': False, 'error': error_msg}
            
            credentials = cred_response.json()
            logger.info("Got upload credentials")
            
            # Step 2: Upload to S3
            logger.info("Uploading to S3...")
            
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['accessKeyId'],
                aws_secret_access_key=credentials['secretAccessKey'],
                aws_session_token=credentials['sessionToken']
            )
            
            file_size = os.path.getsize(geotiff_path)
            logger.info(f"Uploading file: {geotiff_path} ({file_size / 1024 / 1024:.1f} MB)")
            
            try:
                with open(geotiff_path, 'rb') as f:
                    s3_client.put_object(
                        Bucket=credentials['bucket'],
                        Key=credentials['key'],
                        Body=f
                    )
                logger.info("S3 upload complete")
            except NoCredentialsError:
                return {
                    'success': False,
                    'error': 'S3 credentials invalid'
                }
            
            # Step 3: Create the tileset upload
            logger.info("Creating tileset upload...")
            
            upload_url = f"{self.api_base}/uploads/v1/{self.username}?access_token={self.access_token}"
            
            # Properly formatted tileset ID
            full_tileset_id = f"{self.username}.{tileset_id}"
            
            upload_data = {
                "url": f"s3://{credentials['bucket']}/{credentials['key']}",
                "tileset": full_tileset_id,
                "name": f"Wind Animation {tileset_id}",
                "type": "raster"  # Important: specify raster type
            }
            
            upload_response = requests.post(upload_url, json=upload_data)
            
            if upload_response.status_code not in [200, 201]:
                error_msg = f"Failed to create upload: {upload_response.status_code} - {upload_response.text}"
                logger.error(error_msg)
                return {'success': False, 'error': error_msg}
            
            upload_info = upload_response.json()
            upload_id = upload_info.get('id')
            logger.info(f"Upload created with ID: {upload_id}")
            
            # Step 4: Wait for processing
            if upload_id:
                status = self._wait_for_upload(upload_id)
                if status['complete']:
                    return {
                        'success': True,
                        'tileset_id': full_tileset_id,
                        'upload_id': upload_id,
                        'format': 'raster-array'
                    }
                else:
                    return {
                        'success': False,
                        'error': status.get('error', 'Upload processing failed')
                    }
            
            return {
                'success': True,
                'tileset_id': full_tileset_id,
                'format': 'raster-array'
            }
            
        except Exception as e:
            logger.error(f"Error uploading raster: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }
    
    def _wait_for_upload(self, upload_id: str, timeout: int = 300) -> Dict[str, Any]:
        """Wait for upload to complete"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                status_url = f"{self.api_base}/uploads/v1/{self.username}/{upload_id}?access_token={self.access_token}"
                response = requests.get(status_url)
                
                if response.status_code == 200:
                    status = response.json()
                    
                    if status.get('complete'):
                        logger.info(f"Upload completed: {upload_id}")
                        return {'complete': True}
                    elif status.get('error'):
                        logger.error(f"Upload failed: {status.get('error')}")
                        return {'complete': False, 'error': status.get('error')}
                    else:
                        logger.info(f"Upload progress: {status.get('progress', 0)}%")
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error checking upload status: {e}")
                
        return {'complete': False, 'error': 'Upload timeout'}
    
    def create_raster_style(self, tileset_id: str) -> Dict[str, Any]:
        """Create a style for raster-particle visualization"""
        style = {
            "version": 8,
            "name": f"Wind Animation {tileset_id}",
            "sources": {
                "wind-source": {
                    "type": "raster-array",
                    "url": f"mapbox://{self.username}.{tileset_id}",
                    "tileSize": 512
                }
            },
            "layers": [
                {
                    "id": "wind-particles",
                    "type": "raster-particle",
                    "source": "wind-source",
                    "source-layer": "wind",
                    "paint": {
                        "raster-particle-speed-factor": 0.4,
                        "raster-particle-fade-opacity-factor": 0.9,
                        "raster-particle-reset-rate-factor": 0.4,
                        "raster-particle-count": 4000,
                        "raster-particle-max-speed": 40,
                        "raster-particle-color": [
                            "interpolate",
                            ["linear"],
                            ["raster-particle-speed"],
                            0, "rgba(59,130,246,255)",
                            5, "rgba(16,185,129,255)",
                            10, "rgba(52,211,153,255)",
                            15, "rgba(251,191,36,255)",
                            20, "rgba(245,158,11,255)",
                            25, "rgba(239,68,68,255)",
                            30, "rgba(220,38,38,255)",
                            35, "rgba(185,28,28,255)",
                            40, "rgba(153,27,27,255)"
                        ]
                    }
                }
            ]
        }
        
        return style
    
    def get_upload_status(self, upload_id: str) -> Dict[str, Any]:
        """Get the status of an upload"""
        try:
            url = f"{self.api_base}/uploads/v1/{self.username}/{upload_id}?access_token={self.access_token}"
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {'error': f"Failed to get upload status: {response.status_code}"}
                
        except Exception as e:
            return {'error': str(e)}