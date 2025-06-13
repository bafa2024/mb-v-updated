# app.py - Weather Visualization Application with Vector/Raster Support and Client-Side Animation
from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import xarray as xr
import numpy as np
import os
import sys
import json
import tempfile
import traceback
from pathlib import Path
from typing import Optional, Dict, List
import aiofiles
from datetime import datetime
import logging
import asyncio
from pydantic import BaseModel
from dotenv import load_dotenv

# Fix for Windows path issues
if sys.platform == "win32":
    import pathlib
    temp = pathlib.PosixPath
    pathlib.PosixPath = pathlib.WindowsPath

# Load environment variables
load_dotenv()

# Import modules
from tileset_management import MapboxTilesetManager
from mts_raster_manager import MTSRasterManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Weather Visualization Platform", version="4.0.0")

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
    BASE_DIR = Path(__file__).parent.absolute()
    UPLOAD_DIR = BASE_DIR / "uploads"
    PROCESSED_DIR = BASE_DIR / "processed"
    RECIPE_DIR = BASE_DIR / "recipes"
    STATIC_DIR = BASE_DIR / "static"
    TEMPLATES_DIR = BASE_DIR / "templates"
    MAX_FILE_SIZE = int(os.getenv("MAX_UPLOAD_SIZE", "500")) * 1024 * 1024  # MB to bytes
    
    # Load Mapbox credentials
    MAPBOX_USERNAME = os.getenv("MAPBOX_USERNAME", "")
    MAPBOX_TOKEN = os.getenv("MAPBOX_TOKEN", "")
    MAPBOX_PUBLIC_TOKEN = os.getenv("MAPBOX_PUBLIC_TOKEN", "")
    
    # Use public token if available, otherwise fall back to main token
    if not MAPBOX_PUBLIC_TOKEN and MAPBOX_TOKEN:
        MAPBOX_PUBLIC_TOKEN = MAPBOX_TOKEN

# Create directories
for dir_path in [Config.UPLOAD_DIR, Config.PROCESSED_DIR, Config.RECIPE_DIR, 
                 Config.STATIC_DIR, Config.TEMPLATES_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Mount static files - fix path
if Config.STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(Config.STATIC_DIR)), name="static")
else:
    logger.warning(f"Static directory not found: {Config.STATIC_DIR}")

templates = Jinja2Templates(directory=str(Config.TEMPLATES_DIR))

# Log configuration
logger.info(f"Base directory: {Config.BASE_DIR}")
logger.info(f"Static directory: {Config.STATIC_DIR}")
logger.info(f"Templates directory: {Config.TEMPLATES_DIR}")
logger.info(f"Mapbox Username: {Config.MAPBOX_USERNAME}")
logger.info(f"Mapbox Token configured: {'Yes' if Config.MAPBOX_TOKEN else 'No'}")
logger.info(f"Mapbox Public Token configured: {'Yes' if Config.MAPBOX_PUBLIC_TOKEN else 'No'}")

# Models
class ProcessingStatus(BaseModel):
    job_id: str
    status: str
    message: str
    tileset_id: Optional[str] = None
    visualization_url: Optional[str] = None
    error: Optional[str] = None

# In-memory storage
active_visualizations = {}
active_sessions = {}  # Store session data for client-side animation

# Default weather tileset
DEFAULT_TILESET = {
    "id": "mapbox.gfs-winds",
    "name": "Global Weather Data (Default)",
    "type": "default",
    "format": "raster-array"
}

@app.get("/", response_class=HTMLResponse)
async def main_page(request: Request):
    """Main page with weather visualization"""
    # Get list of available tilesets
    available_tilesets = []
    
    # Add default Mapbox weather data
    available_tilesets.append(DEFAULT_TILESET)
    
    # Add user's uploaded tilesets
    if Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME:
        try:
            manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            user_tilesets = manager.list_tilesets(limit=50)
            
            for ts in user_tilesets:
                # Include weather-related tilesets
                tileset_name = ts.get('name', '').lower()
                tileset_id = ts.get('id', '')
                
                if any(keyword in tileset_name or keyword in tileset_id.lower() 
                      for keyword in ['weather', 'netcdf', 'wx_', 'wind', 'flow', 'raster']):
                    
                    tileset_info = {
                        "id": ts['id'],
                        "name": ts.get('name', ts['id']),
                        "type": "user",
                        "created": ts.get('created', ''),
                        "modified": ts.get('modified', '')
                    }
                    
                    # Check if we have recipe info
                    tileset_short_id = tileset_id.split('.')[-1] if '.' in tileset_id else tileset_id
                    recipe_files = list(Config.RECIPE_DIR.glob(f"*{tileset_short_id}*.json"))
                    
                    if recipe_files:
                        try:
                            with open(recipe_files[0], 'r') as f:
                                recipe_data = json.load(f)
                                tileset_info['format'] = recipe_data.get('format', 'vector')
                                tileset_info['source_layer'] = recipe_data.get('source_layer')
                                tileset_info['session_id'] = recipe_data.get('session_id')  # For client-side animation
                        except:
                            tileset_info['format'] = 'vector'
                    else:
                        # Check if it's a raster tileset
                        if 'raster' in ts.get('type', '').lower():
                            tileset_info['format'] = 'raster-array'
                        else:
                            tileset_info['format'] = 'vector'
                    
                    available_tilesets.append(tileset_info)
                    
        except Exception as e:
            logger.error(f"Error fetching user tilesets: {e}")
    
    logger.info(f"Available tilesets: {len(available_tilesets)}")
    
    return templates.TemplateResponse("main_weather_map.html", {
        "request": request,
        "mapbox_token": Config.MAPBOX_PUBLIC_TOKEN,
        "mapbox_username": Config.MAPBOX_USERNAME,
        "available_tilesets": available_tilesets,
        "default_tileset": DEFAULT_TILESET
    })

@app.post("/api/upload-netcdf")
async def upload_netcdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    create_tileset: bool = Form(True),
    tileset_name: Optional[str] = Form(None),
    visualization_type: str = Form("vector")
):
    """Upload and process NetCDF file"""
    
    # Validate file
    if not file.filename.endswith('.nc'):
        raise HTTPException(400, "Only NetCDF (.nc) files are allowed")
    
    # Check file size
    content = await file.read()
    file_size = len(content)
    
    if file_size > Config.MAX_FILE_SIZE:
        raise HTTPException(400, f"File too large. Maximum size is {Config.MAX_FILE_SIZE / 1024 / 1024}MB")
    
    # Create job
    job_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Sanitize filename
    safe_filename = Path(file.filename).name
    safe_filename = ''.join(c if c.isalnum() or c in '.-_' else '_' for c in safe_filename)
    if not safe_filename.endswith('.nc'):
        safe_filename = safe_filename.rsplit('.', 1)[0] + '.nc'
    
    # Save uploaded file - use Path properly
    file_path = Config.UPLOAD_DIR / f"{job_id}_{safe_filename}"
    
    # Ensure upload directory exists
    Config.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Saving uploaded file: {file_path}")
    
    try:
        async with aiofiles.open(str(file_path), 'wb') as f:
            await f.write(content)
    except Exception as e:
        logger.error(f"Failed to save file: {e}")
        raise HTTPException(500, "Failed to save uploaded file")
    
    # Process file
    try:
        # Analyze file
        result = await process_netcdf_file(
            file_path, job_id, create_tileset, tileset_name, visualization_type
        )
        
        # Store session data for client-side animation
        if result.get('wind_data'):
            active_sessions[job_id] = {
                'file_path': str(file_path),
                'wind_data': result['wind_data'],
                'bounds': result.get('bounds'),
                'created_at': datetime.now().isoformat()
            }
            result['session_id'] = job_id
        
        if create_tileset and Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME:
            # Start background tileset creation
            background_tasks.add_task(
                create_mapbox_tileset_background,
                file_path,
                job_id,
                result.get('tileset_id'),
                visualization_type
            )
            
            result['status'] = 'processing'
            result['message'] = 'File uploaded successfully. Creating Mapbox tileset...'
        
        return JSONResponse(result)
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Clean up file on error
        if file_path.exists():
            try:
                file_path.unlink()
            except:
                pass
                
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

async def process_netcdf_file(file_path: Path, job_id: str, create_tileset: bool, 
                             tileset_name: Optional[str], visualization_type: str) -> Dict:
    """Process NetCDF file and extract metadata"""
    try:
        # Convert Path to string for xarray
        file_path_str = str(file_path)
        ds = xr.open_dataset(file_path_str)
        
        # Log file info
        logger.info(f"Opened NetCDF file: {file_path}")
        logger.info(f"Dimensions: {dict(ds.dims)}")
        logger.info(f"Variables: {list(ds.data_vars)}")
        logger.info(f"Coordinates: {list(ds.coords)}")
        
        # Extract metadata
        metadata = {
            "dimensions": dict(ds.dims),
            "variables": list(ds.data_vars),
            "coordinates": list(ds.coords),
            "attributes": dict(ds.attrs)
        }
        
        # Find wind components
        wind_components = find_wind_components(ds)
        
        # Get all scalar variables
        scalar_vars = []
        vector_pairs = []
        
        if wind_components:
            logger.info(f"Found wind components: {wind_components}")
            vector_pairs.append({
                "name": "wind",
                "u": wind_components["u"],
                "v": wind_components["v"]
            })
            scalar_vars = [v for v in ds.data_vars if v not in [wind_components["u"], wind_components["v"]]]
        else:
            logger.warning("No wind components found in NetCDF file")
            scalar_vars = list(ds.data_vars)
        
        # Get bounds
        bounds = get_dataset_bounds(ds)
        if bounds:
            logger.info(f"Dataset bounds: {bounds}")
        else:
            logger.warning("Could not determine dataset bounds")
        
        # Get data previews
        previews = {}
        for var_name in list(ds.data_vars)[:5]:  # Preview first 5 variables
            try:
                var_data = ds[var_name]
                if 'time' in var_data.dims:
                    var_data = var_data.isel(time=0)
                
                values = var_data.values.flatten()
                values = values[~np.isnan(values)]  # Remove NaN values
                
                if len(values) > 0:
                    previews[var_name] = {
                        "min": float(np.min(values)),
                        "max": float(np.max(values)),
                        "mean": float(np.mean(values)),
                        "units": var_data.attrs.get("units", "unknown")
                    }
            except:
                pass
        
        # Extract wind data for client-side animation
        wind_data = None
        if wind_components and visualization_type in ['raster-array', 'client-side']:
            wind_data = extract_wind_data_for_client(ds, wind_components, bounds)
        
        # Generate tileset ID
        if not tileset_name:
            filename = Path(file_path).stem.split('_', 1)[-1]  # Remove job_id prefix
            tileset_name = ''.join(c for c in filename if c.isalnum() or c in '-_')[:20]
            if not tileset_name:
                tileset_name = "weather_data"
        
        # Sanitize tileset name
        tileset_name = tileset_name.lower()
        tileset_name = ''.join(c if c.isalnum() or c in '-_' else '_' for c in tileset_name)
        tileset_name = '_'.join(part for part in tileset_name.split('_') if part)
        
        # Create short timestamp
        timestamp = datetime.now().strftime("%m%d%H%M")
        prefix = "wx"
        
        # Ensure tileset ID is under 32 chars
        max_name_length = 32 - len(prefix) - len(timestamp) - 2  # 2 for underscores
        if len(tileset_name) > max_name_length:
            tileset_name = tileset_name[:max_name_length]
        
        tileset_id = f"{prefix}_{tileset_name}_{timestamp}"
        tileset_id = tileset_id.lower()
        tileset_id = ''.join(c for c in tileset_id if c.isalnum() or c in '-_')
        tileset_id = tileset_id[:32].rstrip('_')
        
        logger.info(f"Generated tileset_id: {tileset_id}")
        
        # Store visualization info with requested type
        active_visualizations[job_id] = {
            "file_path": str(file_path),
            "tileset_id": tileset_id,
            "metadata": metadata,
            "wind_components": wind_components,
            "bounds": bounds,
            "visualization_type": visualization_type,
            "requested_format": "raster-array" if visualization_type == "raster-array" else "vector",
            "created_at": datetime.now().isoformat(),
            "status": "processing",
            "scalar_vars": scalar_vars,
            "vector_pairs": vector_pairs,
            "session_id": job_id  # Link to session for client-side animation
        }
        
        ds.close()
        
        return {
            "success": True,
            "job_id": job_id,
            "tileset_id": tileset_id,
            "metadata": metadata,
            "wind_components": wind_components,
            "bounds": bounds,
            "visualization_type": visualization_type,
            "requested_format": "raster-array" if visualization_type == "raster-array" else "vector",
            "scalar_vars": scalar_vars,
            "vector_pairs": vector_pairs,
            "previews": previews,
            "wind_data": wind_data,
            "session_id": job_id
        }
        
    except Exception as e:
        logger.error(f"Error in process_netcdf_file: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Try to provide helpful error message
        error_msg = str(e)
        if "No such file" in error_msg:
            error_msg = "NetCDF file could not be read. Please ensure it's a valid NetCDF format."
        elif "decode" in error_msg:
            error_msg = "NetCDF file encoding error. The file may be corrupted."
        
        raise Exception(error_msg)

def extract_wind_data_for_client(ds, wind_components, bounds):
    """Extract wind data in a format suitable for client-side animation"""
    try:
        u_var = ds[wind_components['u']]
        v_var = ds[wind_components['v']]
        
        # Handle time dimension
        if 'time' in u_var.dims:
            u_var = u_var.isel(time=0)
            v_var = v_var.isel(time=0)
        
        # Get coordinate arrays
        lats = ds.lat.values if 'lat' in ds else ds.latitude.values
        lons = ds.lon.values if 'lon' in ds else ds.longitude.values
        
        # Subsample if data is too large (max 150x150 for performance)
        max_points = 150
        lat_step = max(1, len(lats) // max_points)
        lon_step = max(1, len(lons) // max_points)
        
        lats_sub = lats[::lat_step]
        lons_sub = lons[::lon_step]
        u_sub = u_var.values[::lat_step, ::lon_step]
        v_sub = v_var.values[::lat_step, ::lon_step]
        
        # Handle NaN values
        u_sub = np.nan_to_num(u_sub, nan=0.0)
        v_sub = np.nan_to_num(v_sub, nan=0.0)
        
        # Calculate speed
        speed = np.sqrt(u_sub**2 + v_sub**2)
        
        return {
            "grid": {
                "lats": lats_sub.tolist(),
                "lons": lons_sub.tolist(),
                "shape": list(u_sub.shape)
            },
            "u_component": u_sub.tolist(),
            "v_component": v_sub.tolist(),
            "speed": speed.tolist(),
            "metadata": {
                "units": u_var.attrs.get('units', 'm/s')
            }
        }
    except Exception as e:
        logger.error(f"Error extracting wind data for client: {e}")
        return None

def find_wind_components(ds):
    """Find U and V wind components in dataset"""
    u_patterns = ['u', 'u10', 'u_wind', 'u_component', 'eastward', 'ugrd', 'u-component', 'uas']
    v_patterns = ['v', 'v10', 'v_wind', 'v_component', 'northward', 'vgrd', 'v-component', 'vas']
    
    u_var = None
    v_var = None
    
    for var in ds.data_vars:
        var_lower = var.lower()
        if not u_var and any(p in var_lower for p in u_patterns):
            u_var = var
        elif not v_var and any(p in var_lower for p in v_patterns):
            v_var = var
    
    if u_var and v_var:
        return {"u": u_var, "v": v_var}
    return None

def get_dataset_bounds(ds):
    """Extract geographic bounds from dataset"""
    try:
        # Find lat/lon coordinates
        lat_names = ['lat', 'latitude', 'y', 'Y']
        lon_names = ['lon', 'longitude', 'x', 'X']
        
        lat_coord = None
        lon_coord = None
        
        for name in lat_names:
            if name in ds.coords:
                lat_coord = ds.coords[name]
                break
                
        for name in lon_names:
            if name in ds.coords:
                lon_coord = ds.coords[name]
                break
        
        if lat_coord is not None and lon_coord is not None:
            return {
                "north": float(lat_coord.max()),
                "south": float(lat_coord.min()),
                "east": float(lon_coord.max()),
                "west": float(lon_coord.min())
            }
    except:
        pass
    
    return None

async def create_mapbox_tileset_background(file_path: Path, job_id: str, 
                                          tileset_id: str, visualization_type: str):
    """Background task to create Mapbox tileset with proper error handling"""
    try:
        if not Config.MAPBOX_TOKEN:
            logger.error("Mapbox token not configured")
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = 'Mapbox token not configured'
            return
        
        # Convert Path to string
        file_path_str = str(file_path)
        
        # Verify file exists
        if not os.path.exists(file_path_str):
            logger.error(f"NetCDF file not found: {file_path_str}")
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = 'Input file not found'
            return
        
        # Get the requested format from active_visualizations
        requested_format = active_visualizations[job_id].get('requested_format', 'vector')
        
        logger.info(f"Creating {requested_format} tileset from {file_path_str}")
        
        # Initialize variables
        actual_format = None
        result = None
        
        # Check if raster-array was requested
        if requested_format == 'raster-array' and Config.MAPBOX_TOKEN:
            # First try raster-array if requested
            logger.info("Attempting to create raster-array tileset...")
            
            # Import MTS Raster Manager
            raster_manager = MTSRasterManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            
            # Try to create raster tileset
            result = await raster_manager.create_raster_tileset(file_path_str, tileset_id)
            
            if result['success']:
                actual_format = 'raster-array'
                # Update visualization info
                if job_id in active_visualizations:
                    active_visualizations[job_id]['mapbox_tileset'] = result['tileset_id']
                    active_visualizations[job_id]['status'] = 'completed'
                    active_visualizations[job_id]['format'] = 'raster-array'
                    active_visualizations[job_id]['actual_format'] = 'raster-array'
                    active_visualizations[job_id]['requested_format'] = 'raster-array'
                    active_visualizations[job_id]['source_layer'] = result.get('source_layer', '10winds')
                    active_visualizations[job_id]['recipe_id'] = result.get('recipe_id')
                    active_visualizations[job_id]['publish_job_id'] = result.get('publish_job_id')
                    
                    # Save recipe info with proper format
                    save_recipe_info(tileset_id, result, active_visualizations[job_id])
                    
                logger.info("Successfully created raster-array tileset")
                return
            else:
                # Check if it's a Pro account issue
                if result.get('fallback_to_vector', False) or result.get('error_code') == 422:
                    logger.warning("Raster-array requires Pro account, falling back to vector")
                    if job_id in active_visualizations:
                        active_visualizations[job_id]['warning'] = result.get('error', 'Falling back to vector format')
                        active_visualizations[job_id]['use_client_animation'] = True  # Flag for client-side animation
                    actual_format = 'vector'  # Will fall back to vector
                else:
                    # Some other error occurred
                    logger.error(f"Raster tileset creation failed: {result.get('error')}")
                    if job_id in active_visualizations:
                        active_visualizations[job_id]['error'] = result.get('error')
                        active_visualizations[job_id]['status'] = 'failed'
                    return
        
        # Fall back to vector format (or if vector was requested)
        if actual_format != 'raster-array':
            logger.info("Creating vector tileset...")
            
            manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            
            # Process NetCDF to tileset
            result = manager.process_netcdf_to_tileset(file_path_str, tileset_id)
            
            if result['success']:
                actual_format = 'vector'
                # Update visualization info
                if job_id in active_visualizations:
                    active_visualizations[job_id]['mapbox_tileset'] = result['tileset_id']
                    active_visualizations[job_id]['status'] = 'completed'
                    active_visualizations[job_id]['format'] = 'vector'
                    active_visualizations[job_id]['actual_format'] = 'vector'
                    active_visualizations[job_id]['source_layer'] = result.get('source_layer', 'weather_data')
                    active_visualizations[job_id]['recipe_id'] = result.get('recipe_id')
                    active_visualizations[job_id]['publish_job_id'] = result.get('publish_job_id')
                    
                    # Add warning if raster was requested but vector was created
                    if requested_format == 'raster-array':
                        active_visualizations[job_id]['format_fallback'] = True
                        active_visualizations[job_id]['warning'] = 'Created vector format (raster-array requires Pro account)'
                        active_visualizations[job_id]['use_client_animation'] = True
                    
                    # Save recipe info with correct formats
                    save_recipe_info(tileset_id, result, active_visualizations[job_id])
                        
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Tileset creation failed: {error_msg}")
                
                if job_id in active_visualizations:
                    active_visualizations[job_id]['status'] = 'failed'
                    active_visualizations[job_id]['error'] = error_msg
                
    except Exception as e:
        logger.error(f"Error creating tileset: {str(e)}")
        import traceback
        traceback.print_exc()
        
        if job_id in active_visualizations:
            active_visualizations[job_id]['status'] = 'failed'
            active_visualizations[job_id]['error'] = str(e)

def save_recipe_info(tileset_id: str, result: Dict, viz_info: Dict):
    """Save recipe information for future reference"""
    recipe_path = Config.RECIPE_DIR / f"recipe_{tileset_id}.json"
    
    # Ensure we capture the actual format that was created
    actual_format = result.get('format', 'vector')
    if 'raster' in str(result.get('tileset_id', '')).lower() or result.get('format') == 'raster-array':
        actual_format = 'raster-array'
    
    recipe_data = {
        "tileset_id": tileset_id,
        "mapbox_tileset": result['tileset_id'],
        "created": datetime.now().isoformat(),
        "format": actual_format,  # The actual format created
        "actual_format": actual_format,  # Explicitly store actual format
        "requested_format": viz_info.get('requested_format', 'vector'),  # What was requested
        "source_layer": result.get('source_layer', 'weather_data' if actual_format == 'vector' else '10winds'),
        "recipe_id": result.get('recipe_id'),
        "publish_job_id": result.get('publish_job_id'),
        "scalar_vars": viz_info.get("scalar_vars", []),
        "vector_pairs": viz_info.get("vector_pairs", []),
        "visualization_type": viz_info.get('visualization_type', 'vector'),
        "is_raster_array": actual_format == 'raster-array',  # Explicit flag
        "use_client_animation": viz_info.get('use_client_animation', False),
        "session_id": viz_info.get('session_id')  # Store session ID for client-side animation
    }
    
    try:
        with open(str(recipe_path), 'w') as f:
            json.dump(recipe_data, f, indent=2)
        logger.info(f"Saved recipe info to {recipe_path}")
    except Exception as e:
        logger.error(f"Failed to save recipe: {e}")

@app.get("/api/visualization-status/{job_id}")
async def get_visualization_status(job_id: str):
    """Get status of visualization processing"""
    if job_id not in active_visualizations:
        raise HTTPException(404, "Job not found")
    
    viz_info = active_visualizations[job_id]
    
    return JSONResponse({
        "job_id": job_id,
        "status": viz_info.get('status', 'processing'),
        "tileset_id": viz_info.get('tileset_id'),
        "mapbox_tileset": viz_info.get('mapbox_tileset'),
        "error": viz_info.get('error'),
        "warning": viz_info.get('warning'),
        "metadata": viz_info.get('metadata'),
        "format": viz_info.get('format', 'vector'),
        "actual_format": viz_info.get('actual_format', viz_info.get('format', 'vector')),
        "requested_format": viz_info.get('requested_format', 'vector'),
        "wind_components": viz_info.get('wind_components'),
        "scalar_vars": viz_info.get('scalar_vars', []),
        "vector_pairs": viz_info.get('vector_pairs', []),
        "source_layer": viz_info.get('source_layer'),
        "publish_job_id": viz_info.get('publish_job_id'),
        "visualization_type": viz_info.get('visualization_type', 'vector'),
        "use_client_animation": viz_info.get('use_client_animation', False),
        "session_id": viz_info.get('session_id')
    })

@app.get("/api/tileset-status/{username}/{tileset_id}")
async def get_tileset_publish_status(username: str, tileset_id: str):
    """Check the publish status of a tileset"""
    if not Config.MAPBOX_TOKEN:
        raise HTTPException(500, "Mapbox token not configured")
    
    try:
        manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, username)
        status = manager.get_tileset_status(tileset_id)
        
        # Also check for any active publishing jobs
        if 'publishing' in status:
            return JSONResponse({
                "status": "publishing",
                "complete": False
            })
        
        return JSONResponse({
            "status": "ready",
            "complete": True,
            "tileset_info": status
        })
        
    except Exception as e:
        logger.error(f"Error getting tileset status: {e}")
        return JSONResponse({
            "status": "error",
            "error": str(e)
        })

@app.post("/api/load-tileset")
async def load_tileset(tileset_id: str = Form(...)):
    """Load a specific tileset for visualization"""
    try:
        # Check if it's a default tileset
        if tileset_id == DEFAULT_TILESET['id']:
            return JSONResponse({
                "success": True,
                "tileset_id": tileset_id,
                "type": "default",
                "format": "raster-array",
                "actual_format": "raster-array",
                "config": {
                    "layers": ["wind"],
                    "wind_source": tileset_id,
                    "source_layer": "10winds"
                }
            })
        
        # For user tilesets, check for recipe
        tileset_name = tileset_id.split('.')[-1] if '.' in tileset_id else tileset_id
        recipe_files = list(Config.RECIPE_DIR.glob(f"*{tileset_name}*.json"))
        
        format_type = 'vector'  # Default
        actual_format = 'vector'
        requested_format = 'vector'
        source_layer = 'weather_data'
        layer_config = {}
        scalar_vars = []
        vector_pairs = []
        visualization_type = 'vector'
        is_raster_array = False
        use_client_animation = False
        session_id = None
        
        if recipe_files:
            try:
                with open(recipe_files[0], 'r') as f:
                    recipe_data = json.load(f)
                    format_type = recipe_data.get('format', 'vector')
                    actual_format = recipe_data.get('actual_format', format_type)
                    requested_format = recipe_data.get('requested_format', format_type)
                    source_layer = recipe_data.get('source_layer', 'weather_data')
                    scalar_vars = recipe_data.get('scalar_vars', [])
                    vector_pairs = recipe_data.get('vector_pairs', [])
                    visualization_type = recipe_data.get('visualization_type', 'vector')
                    is_raster_array = recipe_data.get('is_raster_array', False)
                    use_client_animation = recipe_data.get('use_client_animation', False)
                    session_id = recipe_data.get('session_id')
                    
                    # Double-check format based on source layer
                    if source_layer == '10winds' or is_raster_array:
                        actual_format = 'raster-array'
                    
                logger.info(f"Found recipe for {tileset_name}, format: {format_type}, actual: {actual_format}, requested: {requested_format}")
            except Exception as e:
                logger.error(f"Error reading recipe: {e}")
        
        # Check if tileset exists on Mapbox and verify its type
        if Config.MAPBOX_TOKEN:
            manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            tileset_info = manager.get_tileset_status(tileset_name)
            
            if 'error' not in tileset_info:
                # Tileset exists
                logger.info(f"Tileset {tileset_name} exists on Mapbox")
                
                # Check the actual tileset type from Mapbox
                if 'type' in tileset_info:
                    mapbox_type = tileset_info.get('type', '').lower()
                    if 'raster' in mapbox_type:
                        actual_format = 'raster-array'
                        source_layer = '10winds'  # Standard layer for raster wind data
                    else:
                        actual_format = 'vector'
                        source_layer = 'weather_data'
                        
                logger.info(f"Mapbox reports tileset type as: {mapbox_type}, format: {actual_format}")
        
        return JSONResponse({
            "success": True,
            "tileset_id": tileset_id,
            "type": "user",
            "format": actual_format,  # Use the verified actual format
            "actual_format": actual_format,
            "requested_format": requested_format,
            "config": {
                "source_layer": source_layer,
                "visualization_type": visualization_type,
                "scalar_vars": scalar_vars,
                "vector_pairs": vector_pairs,
                "format": actual_format,
                "is_raster_array": actual_format == 'raster-array',
                "use_client_animation": use_client_animation,
                "session_id": session_id
            }
        })
        
    except Exception as e:
        logger.error(f"Error loading tileset: {str(e)}")
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

@app.get("/api/wind-data/{session_id}")
async def get_wind_data(session_id: str):
    """Get wind data for client-side animation"""
    if session_id not in active_sessions:
        # Try to load from active visualizations
        if session_id in active_visualizations:
            viz_info = active_visualizations[session_id]
            file_path = viz_info.get('file_path')
            
            if file_path and os.path.exists(file_path):
                try:
                    # Re-extract wind data
                    ds = xr.open_dataset(file_path)
                    wind_components = viz_info.get('wind_components')
                    bounds = viz_info.get('bounds')
                    
                    if wind_components:
                        wind_data = extract_wind_data_for_client(ds, wind_components, bounds)
                        ds.close()
                        
                        if wind_data:
                            return JSONResponse({
                                "success": True,
                                **wind_data
                            })
                except Exception as e:
                    logger.error(f"Error re-extracting wind data: {e}")
        
        raise HTTPException(404, "Session not found")
    
    session_data = active_sessions[session_id]
    wind_data = session_data.get('wind_data')
    
    if not wind_data:
        raise HTTPException(404, "No wind data available for this session")
    
    return JSONResponse({
        "success": True,
        **wind_data
    })

@app.get("/api/active-visualizations")
async def get_active_visualizations():
    """Get list of active visualizations"""
    return JSONResponse({
        "visualizations": [
            {
                "job_id": job_id,
                "tileset_id": viz.get('tileset_id'),
                "mapbox_tileset": viz.get('mapbox_tileset'),
                "status": viz.get('status', 'processing'),
                "created_at": viz.get('created_at'),
                "format": viz.get('format', 'vector'),
                "actual_format": viz.get('actual_format', viz.get('format', 'vector')),
                "requested_format": viz.get('requested_format', 'vector'),
                "wind_components": viz.get('wind_components'),
                "scalar_vars": viz.get('scalar_vars', []),
                "vector_pairs": viz.get('vector_pairs', []),
                "use_client_animation": viz.get('use_client_animation', False),
                "session_id": viz.get('session_id')
            }
            for job_id, viz in active_visualizations.items()
        ]
    })

@app.delete("/api/visualization/{job_id}")
async def delete_visualization(job_id: str):
    """Delete a visualization and its files"""
    if job_id not in active_visualizations:
        raise HTTPException(404, "Visualization not found")
    
    viz_info = active_visualizations[job_id]
    
    # Delete uploaded file
    try:
        file_path = Path(viz_info['file_path'])
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted file: {file_path}")
    except Exception as e:
        logger.error(f"Error deleting file: {e}")
    
    # Remove from active visualizations
    del active_visualizations[job_id]
    
    # Remove from active sessions if exists
    if job_id in active_sessions:
        del active_sessions[job_id]
    
    return {"success": True, "message": "Visualization deleted"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "mapbox_configured": bool(Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME),
        "mapbox_public_token": bool(Config.MAPBOX_PUBLIC_TOKEN),
        "active_jobs": len(active_visualizations),
        "active_sessions": len(active_sessions),
        "version": "4.0.0"
    }

# Cleanup old files periodically
async def cleanup_old_files():
    """Remove old temporary files"""
    try:
        cutoff_time = datetime.now().timestamp() - (24 * 3600)  # 24 hours
        
        for dir_path in [Config.UPLOAD_DIR, Config.PROCESSED_DIR]:
            for file_path in dir_path.glob("*"):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    file_path.unlink()
                    logger.info(f"Cleaned up old file: {file_path}")
        
        # Clean up old sessions
        to_remove = []
        for session_id, session_data in active_sessions.items():
            created_at = datetime.fromisoformat(session_data.get('created_at', datetime.now().isoformat()))
            if (datetime.now() - created_at).total_seconds() > 24 * 3600:
                to_remove.append(session_id)
        
        for session_id in to_remove:
            del active_sessions[session_id]
            logger.info(f"Cleaned up old session: {session_id}")
                    
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

# Startup event
@app.on_event("startup")
async def startup_event():
    logger.info("Starting Weather Visualization Platform v4.0...")
    logger.info(f"Mapbox Username: {Config.MAPBOX_USERNAME}")
    logger.info(f"Mapbox Token Set: {'Yes' if Config.MAPBOX_TOKEN else 'No'}")
    logger.info(f"Mapbox Public Token Set: {'Yes' if Config.MAPBOX_PUBLIC_TOKEN else 'No'}")
    
    # Run cleanup
    await cleanup_old_files()
    
    # Test Mapbox connection
    if Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME:
        try:
            manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            tilesets = manager.list_tilesets(limit=1)
            logger.info(f"Mapbox connection successful. Found {len(tilesets)} tilesets.")
            
        except Exception as e:
            logger.error(f"Mapbox connection test failed: {e}")

if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment or use default
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    
    uvicorn.run(
        app,
        host=host,
        port=port,
        reload=os.getenv("DEBUG", "False").lower() == "true"
    )