"""
Mapbox Dataset Management Module
Handles creation and management of Mapbox datasets from NetCDF data
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
import uuid
import geojson
from geojson import Feature, FeatureCollection, Point

logger = logging.getLogger(__name__)


class MapboxDatasetManager:
    """Manages Mapbox dataset operations for feature collections"""
    
    def __init__(self, access_token: str, username: str):
        self.access_token = access_token
        self.username = username
        self.api_base = "https://api.mapbox.com"
        
    def create_dataset(self, name: str = None, description: str = None) -> Dict[str, Any]:
        """Create a new empty dataset"""
        try:
            url = f"{self.api_base}/datasets/v1/{self.username}?access_token={self.access_token}"
            
            data = {}
            if name:
                data['name'] = name
            if description:
                data['description'] = description
            
            response = requests.post(url, json=data)
            
            if response.status_code in [200, 201]:
                dataset_info = response.json()
                logger.info(f"Created dataset: {dataset_info.get('id')}")
                return {
                    "success": True,
                    "dataset_id": dataset_info.get('id'),
                    "owner": dataset_info.get('owner'),
                    "created": dataset_info.get('created'),
                    "modified": dataset_info.get('modified')
                }
            else:
                error_msg = f"Failed to create dataset: {response.status_code}"
                if response.text:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', error_msg)
                    except:
                        error_msg += f" - {response.text}"
                
                logger.error(error_msg)
                return {"success": False, "error": error_msg}
                
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def add_features_to_dataset(self, dataset_id: str, features: List[Dict]) -> Dict[str, Any]:
        """Add features to an existing dataset"""
        try:
            success_count = 0
            error_count = 0
            feature_ids = []
            
            for feature in features:
                # Generate unique feature ID if not provided
                if 'id' not in feature:
                    feature['id'] = str(uuid.uuid4())
                
                feature_id = feature['id']
                url = f"{self.api_base}/datasets/v1/{self.username}/{dataset_id}/features/{feature_id}?access_token={self.access_token}"
                
                response = requests.put(url, json=feature)
                
                if response.status_code in [200, 201]:
                    success_count += 1
                    feature_ids.append(feature_id)
                    logger.debug(f"Added feature {feature_id} to dataset {dataset_id}")
                else:
                    error_count += 1
                    logger.error(f"Failed to add feature {feature_id}: {response.status_code}")
            
            logger.info(f"Added {success_count} features to dataset {dataset_id}, {error_count} errors")
            
            return {
                "success": True,
                "dataset_id": dataset_id,
                "features_added": success_count,
                "errors": error_count,
                "feature_ids": feature_ids
            }
            
        except Exception as e:
            logger.error(f"Error adding features to dataset: {str(e)}")
            return {"success": False, "error": str(e)}
    
    def process_netcdf_to_dataset(self, netcdf_path: str, dataset_name: str = None) -> Dict[str, Any]:
        """Process NetCDF file and create a Mapbox dataset with features"""
        try:
            # Step 1: Create dataset
            if not dataset_name:
                dataset_name = f"Weather Data {datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            dataset_result = self.create_dataset(
                name=dataset_name,
                description=f"Weather data imported from NetCDF file"
            )
            
            if not dataset_result['success']:
                return dataset_result
            
            dataset_id = dataset_result['dataset_id']
            
            # Step 2: Convert NetCDF to features
            features = self._convert_netcdf_to_features(netcdf_path)
            
            if not features:
                return {
                    "success": False,
                    "error": "Failed to extract features from NetCDF"
                }
            
            logger.info(f"Extracted {len(features)} features from NetCDF")
            
            # Step 3: Add features to dataset (in batches for better performance)
            batch_size = 100
            total_added = 0
            total_errors = 0
            all_feature_ids = []
            
            for i in range(0, len(features), batch_size):
                batch = features[i:i + batch_size]
                result = self.add_features_to_dataset(dataset_id, batch)
                
                if result['success']:
                    total_added += result['features_added']
                    total_errors += result['errors']
                    all_feature_ids.extend(result['feature_ids'])
                else:
                    logger.error(f"Batch {i//batch_size + 1} failed: {result.get('error')}")
            
            return {
                "success": True,
                "dataset_id": dataset_id,
                "dataset_url": f"https://studio.mapbox.com/datasets/{self.username}/{dataset_id}",
                "total_features": len(features),
                "features_added": total_added,
                "errors": total_errors,
                "feature_ids": all_feature_ids
            }
            
        except Exception as e:
            logger.error(f"Error processing NetCDF to dataset: {str(e)}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}
    
    def _convert_netcdf_to_features(self, netcdf_path: str, max_features: int = 10000) -> List[Dict]:
        """Convert NetCDF data points to GeoJSON features"""
        features = []
        
        try:
            ds = xr.open_dataset(netcdf_path)
            
            # Find coordinates
            lons, lats = self._get_coordinates(ds)
            
            # Find data variables
            data_vars = list(ds.data_vars)
            if not data_vars:
                logger.warning("No data variables found in NetCDF")
                return features
            
            # Find wind components if available
            u_var, v_var = self._find_wind_components(ds)
            
            # Sample data if too large
            total_points = len(lats) * len(lons)
            if total_points > max_features:
                # Calculate step size for sampling
                step = int(np.sqrt(total_points / max_features))
                lat_indices = range(0, len(lats), step)
                lon_indices = range(0, len(lons), step)
            else:
                lat_indices = range(len(lats))
                lon_indices = range(len(lons))
            
            logger.info(f"Sampling {len(lat_indices) * len(lon_indices)} points from {total_points} total")
            
            # Create features
            feature_count = 0
            for i in lat_indices:
                for j in lon_indices:
                    lat = float(lats[i])
                    lon = float(lons[j])
                    
                    # Create properties dictionary
                    properties = {
                        'lat': lat,
                        'lon': lon,
                        'index_i': i,
                        'index_j': j
                    }
                    
                    # Add data from all variables
                    for var_name in data_vars[:10]:  # Limit to first 10 variables
                        try:
                            var_data = ds[var_name]
                            
                            # Handle time dimension
                            if 'time' in var_data.dims:
                                var_data = var_data.isel(time=0)
                            
                            # Get value at this point
                            value = float(var_data.values[i, j])
                            if not np.isnan(value):
                                properties[var_name] = round(value, 4)
                                
                                # Add units if available
                                if 'units' in var_data.attrs:
                                    properties[f"{var_name}_units"] = var_data.attrs['units']
                        except Exception as e:
                            logger.debug(f"Could not extract {var_name} at ({i},{j}): {e}")
                    
                    # Calculate wind speed and direction if available
                    if u_var and v_var and u_var in properties and v_var in properties:
                        u_val = properties[u_var]
                        v_val = properties[v_var]
                        speed = float(np.sqrt(u_val**2 + v_val**2))
                        direction = float(np.arctan2(v_val, u_val) * 180 / np.pi)
                        
                        properties['wind_speed'] = round(speed, 4)
                        properties['wind_direction'] = round(direction, 1)
                    
                    # Create GeoJSON feature
                    feature = {
                        "type": "Feature",
                        "geometry": {
                            "type": "Point",
                            "coordinates": [lon, lat]
                        },
                        "properties": properties,
                        "id": f"point_{i}_{j}"
                    }
                    
                    features.append(feature)
                    feature_count += 1
                    
                    if feature_count >= max_features:
                        break
                
                if feature_count >= max_features:
                    break
            
            ds.close()
            logger.info(f"Created {len(features)} features from NetCDF")
            
        except Exception as e:
            logger.error(f"Error converting NetCDF to features: {str(e)}")
            import traceback
            traceback.print_exc()
        
        return features
    
    def _find_wind_components(self, ds) -> Tuple[Optional[str], Optional[str]]:
        """Find U and V wind components in dataset"""
        u_patterns = ['u', 'u10', 'u_wind', 'u_component', 'eastward', 'ugrd', 'uas']
        v_patterns = ['v', 'v10', 'v_wind', 'v_component', 'northward', 'vgrd', 'vas']
        
        u_var = None
        v_var = None
        
        for var in ds.data_vars:
            var_lower = var.lower()
            
            if not u_var:
                for pattern in u_patterns:
                    if pattern in var_lower:
                        u_var = var
                        break
            
            if not v_var:
                for pattern in v_patterns:
                    if pattern in var_lower:
                        v_var = var
                        break
            
            if u_var and v_var:
                break
        
        return u_var, v_var
    
    def _get_coordinates(self, ds) -> Tuple[np.ndarray, np.ndarray]:
        """Extract longitude and latitude coordinates"""
        lon_names = ['lon', 'longitude', 'x', 'X']
        lat_names = ['lat', 'latitude', 'y', 'Y']
        
        lons = None
        lats = None
        
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
    
    def get_dataset_info(self, dataset_id: str) -> Dict[str, Any]:
        """Get information about a dataset"""
        try:
            url = f"{self.api_base}/datasets/v1/{self.username}/{dataset_id}?access_token={self.access_token}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"error": "Dataset not found"}
            else:
                return {"error": f"Failed to get dataset info: {response.status_code}"}
                
        except Exception as e:
            logger.error(f"Error getting dataset info: {str(e)}")
            return {"error": str(e)}
    
    def list_datasets(self, limit: int = 100) -> List[Dict[str, Any]]:
        """List user's datasets"""
        try:
            url = f"{self.api_base}/datasets/v1/{self.username}?access_token={self.access_token}&limit={limit}"
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to list datasets: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error listing datasets: {str(e)}")
            return []
    
    def delete_dataset(self, dataset_id: str) -> bool:
        """Delete a dataset"""
        try:
            url = f"{self.api_base}/datasets/v1/{self.username}/{dataset_id}?access_token={self.access_token}"
            
            response = requests.delete(url)
            
            if response.status_code == 204:
                logger.info(f"Successfully deleted dataset: {dataset_id}")
                return True
            else:
                logger.error(f"Failed to delete dataset: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting dataset: {str(e)}")
            return False
    
    def export_dataset_to_tileset(self, dataset_id: str, tileset_id: str) -> Dict[str, Any]:
        """Export a dataset to a tileset"""
        try:
            # This would typically involve creating a tileset from the dataset
            # For now, returning a placeholder as this requires additional Mapbox API setup
            return {
                "success": True,
                "message": "Dataset export to tileset requires additional configuration",
                "dataset_id": dataset_id,
                "tileset_id": tileset_id
            }
            
        except Exception as e:
            logger.error(f"Error exporting dataset to tileset: {str(e)}")
            return {"success": False, "error": str(e)}