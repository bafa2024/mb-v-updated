#!/usr/bin/env python3
"""
Create a sample NetCDF file with wind data for testing
This creates a simple wind field that should work with the app
"""

import numpy as np
import xarray as xr
from datetime import datetime, timedelta

def create_sample_wind_netcdf(filename="sample_wind_data.nc"):
    """Create a sample NetCDF file with wind components"""
    
    print(f"Creating sample NetCDF file: {filename}")
    
    # Define dimensions
    lat = np.linspace(-90, 90, 73)  # 2.5 degree resolution
    lon = np.linspace(-180, 180, 144)  # 2.5 degree resolution
    time = [datetime(2024, 6, 8, 0, 0)]  # Single time step
    
    # Create coordinate arrays
    lon_grid, lat_grid = np.meshgrid(lon, lat)
    
    # Create synthetic wind field
    # U component (eastward wind)
    # Create a jet stream pattern
    u_wind = 20 * np.exp(-((lat_grid - 45)**2) / 200) * np.sin(lon_grid * np.pi / 180)
    
    # Add some variation
    u_wind += 5 * np.sin(lat_grid * np.pi / 90) * np.cos(lon_grid * np.pi / 60)
    
    # V component (northward wind) 
    # Create circulation patterns
    v_wind = 10 * np.sin(lon_grid * np.pi / 120) * np.cos(lat_grid * np.pi / 180)
    v_wind += 5 * np.cos(lon_grid * np.pi / 90 + np.pi/4)
    
    # Add some noise for realism
    u_wind += np.random.normal(0, 2, u_wind.shape)
    v_wind += np.random.normal(0, 2, v_wind.shape)
    
    # Create the dataset
    ds = xr.Dataset(
        {
            "u10": xr.DataArray(
                u_wind[np.newaxis, :, :],
                dims=["time", "lat", "lon"],
                coords={"time": time, "lat": lat, "lon": lon},
                attrs={
                    "units": "m/s",
                    "long_name": "10 metre U wind component",
                    "standard_name": "eastward_wind"
                }
            ),
            "v10": xr.DataArray(
                v_wind[np.newaxis, :, :],
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
            "title": "Sample Wind Data for Testing",
            "institution": "Weather Visualization Test",
            "source": "Synthetic data",
            "history": f"Created on {datetime.now()}",
            "Conventions": "CF-1.6"
        }
    )
    
    # Add coordinate attributes
    ds.lat.attrs = {
        "units": "degrees_north",
        "long_name": "latitude",
        "standard_name": "latitude"
    }
    
    ds.lon.attrs = {
        "units": "degrees_east", 
        "long_name": "longitude",
        "standard_name": "longitude"
    }
    
    ds.time.attrs = {
        "long_name": "time",
        "standard_name": "time"
    }
    
    # Save to NetCDF file
    ds.to_netcdf(filename)
    
    # Print summary
    print(f"\nFile created successfully!")
    print(f"Dimensions: {dict(ds.dims)}")
    print(f"Variables: {list(ds.data_vars)}")
    print(f"File size: {np.round(ds.nbytes / 1024 / 1024, 2)} MB")
    
    # Print sample values
    print(f"\nSample wind speeds at center of domain:")
    center_lat = len(lat) // 2
    center_lon = len(lon) // 2
    u_sample = u_wind[center_lat, center_lon]
    v_sample = v_wind[center_lat, center_lon]
    speed_sample = np.sqrt(u_sample**2 + v_sample**2)
    print(f"  U component: {u_sample:.2f} m/s")
    print(f"  V component: {v_sample:.2f} m/s")
    print(f"  Wind speed: {speed_sample:.2f} m/s")
    
    return filename

def create_multiple_forecast_hours(num_hours=6):
    """Create multiple NetCDF files for different forecast hours"""
    
    print(f"Creating {num_hours} forecast hour files...")
    
    filenames = []
    base_time = datetime(2024, 6, 8, 0, 0)
    
    for hour in range(num_hours):
        # Define dimensions
        lat = np.linspace(-90, 90, 73)
        lon = np.linspace(-180, 180, 144)
        forecast_time = base_time + timedelta(hours=hour)
        
        # Create coordinate arrays
        lon_grid, lat_grid = np.meshgrid(lon, lat)
        
        # Create wind field that evolves with time
        # Shift patterns based on hour
        phase_shift = hour * np.pi / 12
        
        u_wind = 20 * np.exp(-((lat_grid - 45)**2) / 200) * np.sin(lon_grid * np.pi / 180 + phase_shift)
        u_wind += 5 * np.sin(lat_grid * np.pi / 90) * np.cos(lon_grid * np.pi / 60 + phase_shift/2)
        
        v_wind = 10 * np.sin(lon_grid * np.pi / 120 + phase_shift) * np.cos(lat_grid * np.pi / 180)
        v_wind += 5 * np.cos(lon_grid * np.pi / 90 + np.pi/4 + phase_shift/3)
        
        # Add some noise
        u_wind += np.random.normal(0, 2, u_wind.shape)
        v_wind += np.random.normal(0, 2, v_wind.shape)
        
        # Create dataset
        ds = xr.Dataset(
            {
                "u10": xr.DataArray(
                    u_wind[np.newaxis, :, :],
                    dims=["time", "lat", "lon"],
                    coords={"time": [forecast_time], "lat": lat, "lon": lon},
                    attrs={
                        "units": "m/s",
                        "long_name": "10 metre U wind component"
                    }
                ),
                "v10": xr.DataArray(
                    v_wind[np.newaxis, :, :],
                    dims=["time", "lat", "lon"],
                    coords={"time": [forecast_time], "lat": lat, "lon": lon},
                    attrs={
                        "units": "m/s",
                        "long_name": "10 metre V wind component"
                    }
                )
            },
            attrs={
                "title": f"Wind Forecast Hour {hour:03d}",
                "forecast_hour": hour,
                "base_time": str(base_time),
                "valid_time": str(forecast_time)
            }
        )
        
        filename = f"wind_forecast_h{hour:03d}.nc"
        ds.to_netcdf(filename)
        filenames.append(filename)
        print(f"  Created: {filename}")
    
    return filenames

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--multiple":
        # Create multiple files for different forecast hours
        num_hours = int(sys.argv[2]) if len(sys.argv) > 2 else 6
        files = create_multiple_forecast_hours(num_hours)
        print(f"\nCreated {len(files)} forecast files")
    else:
        # Create single test file
        create_sample_wind_netcdf()
        print("\nYou can now upload 'sample_wind_data.nc' to test the app")
        print("\nTo create multiple forecast files, run:")
        print("  python create_sample_netcdf.py --multiple 6")