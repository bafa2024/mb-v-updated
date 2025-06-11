#!/usr/bin/env python3
"""
Create a sample NetCDF file with wind data optimized for Mapbox raster-array animation
This creates a properly structured wind field that works with the app
"""

import numpy as np
import xarray as xr
from datetime import datetime, timedelta
import os

def create_sample_wind_netcdf(filename="sample_wind_data.nc", include_multiple_times=False):
    """Create a sample NetCDF file with wind components optimized for raster-array"""
    
    print(f"Creating sample NetCDF file: {filename}")
    
    # Define dimensions - use regular grid for better compatibility
    # Reduced resolution for smaller file size and better processing
    lat = np.linspace(-90, 90, 37)  # 5 degree resolution (37 points)
    lon = np.linspace(-180, 180, 73)  # 5 degree resolution (73 points)
    
    # Time dimension
    if include_multiple_times:
        # Multiple time steps for animation
        base_time = datetime(2024, 1, 1, 0, 0)
        time = [base_time + timedelta(hours=i*6) for i in range(4)]  # 4 time steps, 6 hours apart
    else:
        # Single time step
        time = [datetime(2024, 1, 1, 0, 0)]
    
    print(f"Grid size: {len(lat)} x {len(lon)} x {len(time)} time steps")
    
    # Create coordinate arrays
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    
    # Initialize arrays for all time steps
    u_wind_all = []
    v_wind_all = []
    
    for t_idx, t in enumerate(time):
        # Create synthetic wind field that varies with time
        phase_shift = t_idx * np.pi / 6 if include_multiple_times else 0
        
        # U component (eastward wind)
        # Create a jet stream pattern
        u_wind = 15 * np.exp(-((lat_grid - 40)**2) / 300) * np.sin((lon_grid + phase_shift * 30) * np.pi / 180)
        
        # Add tropical easterlies
        u_wind += -10 * np.exp(-((lat_grid)**2) / 100) * np.cos(lon_grid * np.pi / 180)
        
        # Add some variation
        u_wind += 3 * np.sin(lat_grid * np.pi / 90) * np.cos((lon_grid + phase_shift * 20) * np.pi / 60)
        
        # V component (northward wind) 
        # Create circulation patterns
        v_wind = 8 * np.sin((lon_grid + phase_shift * 40) * np.pi / 120) * np.cos(lat_grid * np.pi / 180)
        v_wind += 4 * np.cos(lon_grid * np.pi / 90 + np.pi/4 + phase_shift)
        
        # Add some controlled noise for realism (less noise for cleaner visualization)
        u_wind += np.random.normal(0, 1, u_wind.shape)
        v_wind += np.random.normal(0, 1, v_wind.shape)
        
        # Ensure no extreme values that might cause issues
        u_wind = np.clip(u_wind, -50, 50)
        v_wind = np.clip(v_wind, -50, 50)
        
        u_wind_all.append(u_wind)
        v_wind_all.append(v_wind)
    
    # Stack arrays for time dimension
    u_wind_all = np.stack(u_wind_all, axis=0)
    v_wind_all = np.stack(v_wind_all, axis=0)
    
    # Create the dataset with proper structure
    ds = xr.Dataset(
        {
            "u10": xr.DataArray(
                u_wind_all.astype(np.float32),  # Use float32 for better compatibility
                dims=["time", "lat", "lon"],
                coords={
                    "time": time, 
                    "lat": lat.astype(np.float32), 
                    "lon": lon.astype(np.float32)
                },
                attrs={
                    "units": "m/s",
                    "long_name": "10 metre U wind component",
                    "standard_name": "eastward_wind",
                    "grid_mapping": "crs"
                }
            ),
            "v10": xr.DataArray(
                v_wind_all.astype(np.float32),
                dims=["time", "lat", "lon"],
                coords={
                    "time": time, 
                    "lat": lat.astype(np.float32), 
                    "lon": lon.astype(np.float32)
                },
                attrs={
                    "units": "m/s",
                    "long_name": "10 metre V wind component",
                    "standard_name": "northward_wind",
                    "grid_mapping": "crs"
                }
            )
        },
        attrs={
            "title": "Sample Wind Data for Raster-Array Animation",
            "institution": "Weather Visualization Test",
            "source": "Synthetic data optimized for Mapbox",
            "history": f"Created on {datetime.now()}",
            "Conventions": "CF-1.6",
            "featureType": "grid"
        }
    )
    
    # Add a CRS variable for better georeferencing
    ds["crs"] = xr.DataArray(
        np.int32(0),
        attrs={
            "grid_mapping_name": "latitude_longitude",
            "long_name": "CRS definition",
            "longitude_of_prime_meridian": 0.0,
            "semi_major_axis": 6378137.0,
            "inverse_flattening": 298.257223563,
            "crs_wkt": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
        }
    )
    
    # Add coordinate attributes with proper CF conventions
    ds.lat.attrs = {
        "units": "degrees_north",
        "long_name": "latitude",
        "standard_name": "latitude",
        "axis": "Y",
        "valid_range": np.array([-90.0, 90.0], dtype=np.float32)
    }
    
    ds.lon.attrs = {
        "units": "degrees_east", 
        "long_name": "longitude",
        "standard_name": "longitude",
        "axis": "X",
        "valid_range": np.array([-180.0, 180.0], dtype=np.float32)
    }
    
    ds.time.attrs = {
        "long_name": "time",
        "standard_name": "time",
        "axis": "T"
    }
    
    # Add bounds variables for better grid cell definition
    lat_bounds = np.zeros((len(lat), 2), dtype=np.float32)
    lon_bounds = np.zeros((len(lon), 2), dtype=np.float32)
    
    # Calculate bounds
    lat_diff = lat[1] - lat[0]
    lon_diff = lon[1] - lon[0]
    
    lat_bounds[:, 0] = lat - lat_diff/2
    lat_bounds[:, 1] = lat + lat_diff/2
    lon_bounds[:, 0] = lon - lon_diff/2
    lon_bounds[:, 1] = lon + lon_diff/2
    
    ds["lat_bounds"] = xr.DataArray(
        lat_bounds,
        dims=["lat", "bounds"],
        attrs={"long_name": "latitude bounds"}
    )
    
    ds["lon_bounds"] = xr.DataArray(
        lon_bounds,
        dims=["lon", "bounds"],
        attrs={"long_name": "longitude bounds"}
    )
    
    # Update coordinate attributes to reference bounds
    ds.lat.attrs["bounds"] = "lat_bounds"
    ds.lon.attrs["bounds"] = "lon_bounds"
    
    # Set encoding for better compression and compatibility
    encoding = {
        "u10": {
            "dtype": "float32",
            "zlib": True,
            "complevel": 4,
            "_FillValue": -9999.0,
            "chunksizes": (1, len(lat), len(lon))  # Chunk by time
        },
        "v10": {
            "dtype": "float32", 
            "zlib": True,
            "complevel": 4,
            "_FillValue": -9999.0,
            "chunksizes": (1, len(lat), len(lon))  # Chunk by time
        },
        "time": {
            "dtype": "float64",
            "units": "hours since 1900-01-01 00:00:00"
        },
        "lat": {"dtype": "float32"},
        "lon": {"dtype": "float32"},
        "lat_bounds": {"dtype": "float32"},
        "lon_bounds": {"dtype": "float32"}
    }
    
    # Save to NetCDF file with proper encoding
    ds.to_netcdf(filename, encoding=encoding, engine='netcdf4')
    
    # Print summary
    print(f"\nFile created successfully: {filename}")
    print(f"Dimensions: {dict(ds.dims)}")
    print(f"Variables: {list(ds.data_vars)}")
    print(f"Coordinates: {list(ds.coords)}")
    
    # Calculate and print file size
    file_size = os.path.getsize(filename) / (1024 * 1024)
    print(f"File size: {file_size:.2f} MB")
    
    # Print data range
    print(f"\nData ranges:")
    print(f"  U wind: {float(u_wind_all.min()):.2f} to {float(u_wind_all.max()):.2f} m/s")
    print(f"  V wind: {float(v_wind_all.min()):.2f} to {float(v_wind_all.max()):.2f} m/s")
    
    # Calculate max wind speed
    max_speed = np.sqrt(u_wind_all**2 + v_wind_all**2).max()
    print(f"  Max wind speed: {float(max_speed):.2f} m/s")
    
    # Print sample values at center
    center_lat_idx = len(lat) // 2
    center_lon_idx = len(lon) // 2
    u_sample = u_wind_all[0, center_lat_idx, center_lon_idx]
    v_sample = v_wind_all[0, center_lat_idx, center_lon_idx]
    speed_sample = np.sqrt(u_sample**2 + v_sample**2)
    
    print(f"\nSample wind at center point (lat={lat[center_lat_idx]:.1f}, lon={lon[center_lon_idx]:.1f}):")
    print(f"  U component: {u_sample:.2f} m/s")
    print(f"  V component: {v_sample:.2f} m/s")
    print(f"  Wind speed: {speed_sample:.2f} m/s")
    print(f"  Wind direction: {np.arctan2(v_sample, u_sample) * 180 / np.pi:.1f}°")
    
    return filename


def create_high_res_sample(filename="sample_wind_highres.nc"):
    """Create a higher resolution sample for testing performance"""
    
    print(f"Creating high-resolution NetCDF file: {filename}")
    
    # Higher resolution grid
    lat = np.linspace(-90, 90, 181)  # 1 degree resolution
    lon = np.linspace(-180, 180, 361)  # 1 degree resolution
    time = [datetime(2024, 1, 1, 0, 0)]
    
    print(f"Grid size: {len(lat)} x {len(lon)}")
    
    # Create coordinate arrays
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    
    # Create more complex wind pattern
    u_wind = 20 * np.exp(-((lat_grid - 45)**2) / 400) * np.sin(lon_grid * np.pi / 180)
    u_wind += -15 * np.exp(-((lat_grid + 45)**2) / 400) * np.sin(lon_grid * np.pi / 180)
    u_wind += 10 * np.sin(lat_grid * np.pi / 60) * np.cos(lon_grid * np.pi / 45)
    
    v_wind = 15 * np.sin(lon_grid * np.pi / 90) * np.cos(lat_grid * np.pi / 180)
    v_wind += 10 * np.cos(lon_grid * np.pi / 120 + np.pi/3) * np.sin(lat_grid * np.pi / 180)
    
    # Less noise for cleaner visualization
    u_wind += np.random.normal(0, 0.5, u_wind.shape)
    v_wind += np.random.normal(0, 0.5, v_wind.shape)
    
    # Clip values
    u_wind = np.clip(u_wind, -60, 60)
    v_wind = np.clip(v_wind, -60, 60)
    
    # Create dataset
    ds = xr.Dataset(
        {
            "u10": xr.DataArray(
                u_wind[np.newaxis, :, :].astype(np.float32),
                dims=["time", "lat", "lon"],
                coords={"time": time, "lat": lat, "lon": lon},
                attrs={
                    "units": "m/s",
                    "long_name": "10 metre U wind component",
                    "standard_name": "eastward_wind"
                }
            ),
            "v10": xr.DataArray(
                v_wind[np.newaxis, :, :].astype(np.float32),
                dims=["time", "lat", "lon"],
                coords={"time": time, "lat": lat, "lon": lon},
                attrs={
                    "units": "m/s",
                    "long_name": "10 metre V wind component",
                    "standard_name": "northward_wind"
                }
            )
        },
        attrs={
            "title": "High Resolution Wind Data Sample",
            "Conventions": "CF-1.6"
        }
    )
    
    # Add coordinate attributes
    ds.lat.attrs = {"units": "degrees_north", "standard_name": "latitude"}
    ds.lon.attrs = {"units": "degrees_east", "standard_name": "longitude"}
    
    # Save with compression
    encoding = {
        "u10": {"zlib": True, "complevel": 4},
        "v10": {"zlib": True, "complevel": 4}
    }
    
    ds.to_netcdf(filename, encoding=encoding)
    
    print(f"Created high-res file: {filename}")
    print(f"File size: {os.path.getsize(filename) / (1024 * 1024):.2f} MB")
    
    return filename


def create_minimal_test_file(filename="minimal_wind.nc"):
    """Create a minimal test file with very simple structure"""
    
    print(f"Creating minimal test NetCDF file: {filename}")
    
    # Very small grid for testing
    lat = np.array([-45, -30, -15, 0, 15, 30, 45], dtype=np.float32)
    lon = np.array([-90, -60, -30, 0, 30, 60, 90], dtype=np.float32)
    time = [datetime(2024, 1, 1)]
    
    # Create simple wind pattern
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    u_wind = 10 * np.ones_like(lon_grid, dtype=np.float32)  # Constant eastward wind
    v_wind = 5 * np.sin(lat_grid * np.pi / 90).astype(np.float32)  # Varying with latitude
    
    # Create dataset
    ds = xr.Dataset(
        {
            "u10": (["time", "lat", "lon"], u_wind[np.newaxis, :, :]),
            "v10": (["time", "lat", "lon"], v_wind[np.newaxis, :, :])
        },
        coords={
            "time": time,
            "lat": lat,
            "lon": lon
        }
    )
    
    # Add attributes
    ds.u10.attrs = {"units": "m/s", "long_name": "U wind component"}
    ds.v10.attrs = {"units": "m/s", "long_name": "V wind component"}
    ds.lat.attrs = {"units": "degrees_north"}
    ds.lon.attrs = {"units": "degrees_east"}
    
    ds.to_netcdf(filename)
    print(f"Created minimal file: {filename}")
    
    return filename


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--animated":
            # Create file with multiple time steps
            create_sample_wind_netcdf("animated_wind_data.nc", include_multiple_times=True)
            print("\nCreated animated wind data with multiple time steps")
            
        elif sys.argv[1] == "--highres":
            # Create high resolution file
            create_high_res_sample()
            print("\nCreated high-resolution wind data")
            
        elif sys.argv[1] == "--minimal":
            # Create minimal test file
            create_minimal_test_file()
            print("\nCreated minimal test file")
            
        elif sys.argv[1] == "--all":
            # Create all types
            create_sample_wind_netcdf()
            create_sample_wind_netcdf("animated_wind_data.nc", include_multiple_times=True)
            create_high_res_sample()
            create_minimal_test_file()
            print("\nCreated all sample files")
            
    else:
        # Create standard test file
        create_sample_wind_netcdf()
        print("\nYou can now upload 'sample_wind_data.nc' to test the app")
        print("\nOther options:")
        print("  python create_netcdf.py --animated   # Create with multiple time steps")
        print("  python create_netcdf.py --highres    # Create high-resolution data")
        print("  python create_netcdf.py --minimal    # Create minimal test file")
        print("  python create_netcdf.py --all        # Create all types")