# app_simplified.py - Simplified Weather Visualization without Mapbox Tilesets
from fastapi import FastAPI, UploadFile, File, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import xarray as xr
import numpy as np
import os
import json
from pathlib import Path
from typing import Optional, Dict, List
import aiofiles
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Weather Visualization Platform", version="2.0.0")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
class Config:
    UPLOAD_DIR = Path("uploads")
    STATIC_DIR = Path("static")
    TEMPLATES_DIR = Path("templates")
    MAX_FILE_SIZE = 500 * 1024 * 1024  # 500MB - increased from 100MB
    
    # Mapbox credentials (only for base map)
    MAPBOX_PUBLIC_TOKEN = os.getenv("MAPBOX_PUBLIC_TOKEN", "")

# Create directories
for dir_path in [Config.UPLOAD_DIR, Config.STATIC_DIR, Config.TEMPLATES_DIR]:
    dir_path.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(Config.STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(Config.TEMPLATES_DIR))

# Store active sessions
active_sessions = {}

@app.get("/", response_class=HTMLResponse)
async def main_page(request: Request):
    """Main page with weather visualization"""
    return templates.TemplateResponse("weather_map_simple.html", {
        "request": request,
        "mapbox_token": Config.MAPBOX_PUBLIC_TOKEN
    })

@app.post("/api/upload-netcdf")
async def upload_netcdf(file: UploadFile = File(...)):
    """Upload and analyze NetCDF file"""
    
    # Validate file
    if not file.filename.endswith('.nc'):
        raise HTTPException(400, "Only NetCDF (.nc) files are allowed")
    
    # Create session ID
    session_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Save file temporarily with chunked reading
    file_path = Config.UPLOAD_DIR / f"{session_id}_{file.filename}"
    
    try:
        # Save file in chunks to handle large files
        async with aiofiles.open(file_path, 'wb') as f:
            chunk_size = 1024 * 1024  # 1MB chunks
            total_size = 0
            
            while chunk := await file.read(chunk_size):
                total_size += len(chunk)
                if total_size > Config.MAX_FILE_SIZE:
                    # Clean up partial file
                    await f.close()
                    file_path.unlink()
                    raise HTTPException(400, f"File too large. Maximum size is {Config.MAX_FILE_SIZE / 1024 / 1024}MB")
                await f.write(chunk)
        
        logger.info(f"Saved file: {file_path} ({total_size / 1024 / 1024:.1f}MB)")
        
        # Analyze NetCDF file
        result = analyze_netcdf(file_path, session_id)
        
        # Store session info
        active_sessions[session_id] = {
            "file_path": str(file_path),
            "metadata": result["metadata"],
            "created_at": datetime.now().isoformat()
        }
        
        return JSONResponse(result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        # Clean up file on error
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(500, f"Error processing file: {str(e)}")

def analyze_netcdf(file_path: Path, session_id: str) -> Dict:
    """Analyze NetCDF file and extract data"""
    try:
        # Open with chunks for large files
        ds = xr.open_dataset(file_path, chunks='auto')
        
        # Extract metadata
        metadata = {
            "dimensions": dict(ds.dims),
            "variables": list(ds.data_vars),
            "coordinates": list(ds.coords),
            "attributes": dict(ds.attrs)
        }
        
        # Find spatial coordinates
        lat_name = None
        lon_name = None
        for coord in ['lat', 'latitude', 'y', 'LAT', 'Latitude']:
            if coord in ds.coords:
                lat_name = coord
                break
        for coord in ['lon', 'longitude', 'x', 'LON', 'Longitude']:
            if coord in ds.coords:
                lon_name = coord
                break
        
        if not lat_name or not lon_name:
            raise ValueError("Could not find latitude/longitude coordinates")
        
        # Get coordinate values
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Identify wind components
        wind_components = find_wind_components(ds)
        
        # Extract first time step of wind data if available
        wind_data = None
        if wind_components:
            u_var = ds[wind_components['u']]
            v_var = ds[wind_components['v']]
            
            # Handle time dimension
            if 'time' in u_var.dims:
                u_var = u_var.isel(time=0)
                v_var = v_var.isel(time=0)
            
            # Get data values with subsampling for large datasets
            if len(lats) > 200 or len(lons) > 200:
                # Subsample for preview
                lat_step = max(1, len(lats) // 100)
                lon_step = max(1, len(lons) // 100)
                
                u_data = u_var.values[::lat_step, ::lon_step]
                v_data = v_var.values[::lat_step, ::lon_step]
                lats_preview = lats[::lat_step]
                lons_preview = lons[::lon_step]
            else:
                u_data = u_var.values
                v_data = v_var.values
                lats_preview = lats
                lons_preview = lons
            
            # Handle NaN values
            u_data = np.nan_to_num(u_data, nan=0.0)
            v_data = np.nan_to_num(v_data, nan=0.0)
            
            # Calculate wind speed and direction
            speed = np.sqrt(u_data**2 + v_data**2)
            direction = np.arctan2(v_data, u_data) * 180 / np.pi
            
            wind_data = {
                "u": u_data.tolist(),
                "v": v_data.tolist(),
                "speed": speed.tolist(),
                "direction": direction.tolist(),
                "units": u_var.attrs.get('units', 'm/s'),
                "preview_lats": lats_preview.tolist(),
                "preview_lons": lons_preview.tolist()
            }
        
        # Get bounds
        bounds = {
            "north": float(np.max(lats)),
            "south": float(np.min(lats)),
            "east": float(np.max(lons)),
            "west": float(np.min(lons))
        }
        
        ds.close()
        
        return {
            "success": True,
            "session_id": session_id,
            "metadata": metadata,
            "coordinates": {
                "lat": {"name": lat_name, "size": len(lats)},
                "lon": {"name": lon_name, "size": len(lons)}
            },
            "bounds": bounds,
            "wind_components": wind_components,
            "wind_data": wind_data
        }
        
    except Exception as e:
        logger.error(f"Error analyzing NetCDF: {str(e)}")
        raise

def find_wind_components(ds: xr.Dataset) -> Optional[Dict[str, str]]:
    """Find U and V wind components in dataset"""
    u_patterns = ['u', 'u10', 'u_wind', 'u_component', 'eastward']
    v_patterns = ['v', 'v10', 'v_wind', 'v_component', 'northward']
    
    variables = list(ds.data_vars)
    
    # Look for matching pairs
    for u_pattern in u_patterns:
        for v_pattern in v_patterns:
            u_matches = [v for v in variables if u_pattern in v.lower()]
            v_matches = [v for v in variables if v_pattern in v.lower()]
            
            if u_matches and v_matches:
                # Try to find corresponding pairs
                for u_var in u_matches:
                    for v_var in v_matches:
                        # Check if they seem to be a pair
                        if u_var.replace(u_pattern, '') == v_var.replace(v_pattern, ''):
                            return {"u": u_var, "v": v_var}
    
    return None

@app.get("/api/wind-data/{session_id}")
async def get_wind_data(
    session_id: str,
    time_index: int = 0,
    level_index: Optional[int] = None
):
    """Get wind data for specific time/level"""
    if session_id not in active_sessions:
        raise HTTPException(404, "Session not found")
    
    try:
        file_path = Path(active_sessions[session_id]["file_path"])
        ds = xr.open_dataset(file_path)
        
        # Find wind components
        wind_components = find_wind_components(ds)
        if not wind_components:
            raise HTTPException(404, "No wind data found")
        
        # Get wind data
        u_var = ds[wind_components['u']]
        v_var = ds[wind_components['v']]
        
        # Handle dimensions
        if 'time' in u_var.dims:
            u_var = u_var.isel(time=time_index)
            v_var = v_var.isel(time=time_index)
        
        if level_index is not None and 'level' in u_var.dims:
            u_var = u_var.isel(level=level_index)
            v_var = v_var.isel(level=level_index)
        
        # Get coordinate names
        lat_name = None
        lon_name = None
        for coord in ['lat', 'latitude', 'y']:
            if coord in ds.coords:
                lat_name = coord
                break
        for coord in ['lon', 'longitude', 'x']:
            if coord in ds.coords:
                lon_name = coord
                break
        
        # Create grid data for wind animation
        lats = ds[lat_name].values
        lons = ds[lon_name].values
        
        # Subsample if data is too large
        max_points = 150  # Increased from 100 for better resolution
        lat_step = max(1, len(lats) // max_points)
        lon_step = max(1, len(lons) // max_points)
        
        lats_sub = lats[::lat_step]
        lons_sub = lons[::lon_step]
        u_sub = u_var.values[::lat_step, ::lon_step]
        v_sub = v_var.values[::lat_step, ::lon_step]
        
        # Calculate wind speed
        speed = np.sqrt(u_sub**2 + v_sub**2)
        
        # Handle NaN values
        u_sub = np.nan_to_num(u_sub, nan=0.0)
        v_sub = np.nan_to_num(v_sub, nan=0.0)
        speed = np.nan_to_num(speed, nan=0.0)
        
        ds.close()
        
        return JSONResponse({
            "success": True,
            "grid": {
                "lats": lats_sub.tolist(),
                "lons": lons_sub.tolist(),
                "shape": list(u_sub.shape)
            },
            "u_component": u_sub.tolist(),
            "v_component": v_sub.tolist(),
            "speed": speed.tolist(),
            "metadata": {
                "units": u_var.attrs.get('units', 'm/s'),
                "time_index": time_index,
                "level_index": level_index
            }
        })
        
    except Exception as e:
        logger.error(f"Error getting wind data: {str(e)}")
        raise HTTPException(500, str(e))

@app.delete("/api/session/{session_id}")
async def delete_session(session_id: str):
    """Clean up session data"""
    if session_id in active_sessions:
        # Delete file
        file_path = Path(active_sessions[session_id]["file_path"])
        if file_path.exists():
            file_path.unlink()
        
        # Remove from sessions
        del active_sessions[session_id]
        
        return {"success": True}
    
    raise HTTPException(404, "Session not found")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "active_sessions": len(active_sessions),
        "version": "2.0.0"
    }

# Cleanup old files on startup
@app.on_event("startup")
async def startup_event():
    """Clean up old files on startup"""
    for file_path in Config.UPLOAD_DIR.glob("*"):
        if file_path.is_file():
            try:
                # Delete files older than 1 hour
                if (datetime.now().timestamp() - file_path.stat().st_mtime) > 3600:
                    file_path.unlink()
                    logger.info(f"Cleaned up old file: {file_path}")
            except Exception as e:
                logger.error(f"Error cleaning up {file_path}: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)