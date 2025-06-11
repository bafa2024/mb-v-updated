# app.py - Weather Visualization Application with Vector/Raster Support
from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
import xarray as xr
import numpy as np
import os
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

# Load environment variables
load_dotenv()

# Import modules
from tileset_management import MapboxTilesetManager

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
    UPLOAD_DIR = Path("uploads")
    PROCESSED_DIR = Path("processed")
    RECIPE_DIR = Path("recipes")
    STATIC_DIR = Path("static")
    TEMPLATES_DIR = Path("templates")
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
    dir_path.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(Config.STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(Config.TEMPLATES_DIR))

# Log configuration
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
                      for keyword in ['weather', 'netcdf', 'wx_', 'wind', 'flow']):
                    
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
                        except:
                            tileset_info['format'] = 'vector'
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
    
    # Save uploaded file
    file_path = Config.UPLOAD_DIR / f"{job_id}_{safe_filename}"
    
    logger.info(f"Saving uploaded file: {file_path}")
    
    async with aiofiles.open(file_path, 'wb') as f:
        await f.write(content)
    
    # Process file
    try:
        # Analyze file
        result = await process_netcdf_file(
            file_path, job_id, create_tileset, tileset_name, visualization_type
        )
        
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
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

async def process_netcdf_file(file_path: Path, job_id: str, create_tileset: bool, 
                             tileset_name: Optional[str], visualization_type: str) -> Dict:
    """Process NetCDF file and extract metadata"""
    try:
        ds = xr.open_dataset(file_path)
        
        # Extract metadata
        metadata = {
            "dimensions": dict(ds.dims),
            "variables": list(ds.data_vars),
            "coordinates": list(ds.coords),
            "attributes": dict(ds.attrs)
        }
        
        # Find wind components
        wind_components = find_wind_components(ds)
        
        # Get bounds
        bounds = get_dataset_bounds(ds)
        
        # Generate tileset ID
        if not tileset_name:
            filename = Path(file_path).stem.split('_', 1)[-1]  # Remove job_id prefix
            tileset_name = ''.join(c for c in filename if c.isalnum() or c in '-_')
            if not tileset_name:
                tileset_name = "weather_data"
        
        # Create tileset ID
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
        
        # Store visualization info
        active_visualizations[job_id] = {
            "file_path": str(file_path),
            "tileset_id": tileset_id,
            "metadata": metadata,
            "wind_components": wind_components,
            "bounds": bounds,
            "visualization_type": visualization_type,
            "created_at": datetime.now().isoformat()
        }
        
        ds.close()
        
        return {
            "success": True,
            "job_id": job_id,
            "tileset_id": tileset_id,
            "metadata": metadata,
            "wind_components": wind_components,
            "bounds": bounds,
            "visualization_type": visualization_type
        }
        
    except Exception as e:
        logger.error(f"Error in process_netcdf_file: {str(e)}")
        raise

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
    """Background task to create Mapbox tileset"""
    try:
        if not Config.MAPBOX_TOKEN:
            logger.error("Mapbox token not configured")
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = 'Mapbox token not configured'
            return
        
        manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
        
        # Create tileset based on visualization type
        if visualization_type == 'raster-array':
            # Try raster-array format (may fail on free accounts)
            result = manager.create_raster_array_tileset(str(file_path), tileset_id)
        else:
            # Use vector format (more reliable)
            result = manager.process_netcdf_to_tileset(str(file_path), tileset_id, {})
        
        if result['success']:
            # Update visualization info
            if job_id in active_visualizations:
                active_visualizations[job_id]['mapbox_tileset'] = result['tileset_id']
                active_visualizations[job_id]['status'] = 'completed'
                active_visualizations[job_id]['format'] = result.get('format', visualization_type)
                
                # Save recipe info
                recipe_path = Config.RECIPE_DIR / f"recipe_{tileset_id}.json"
                recipe_data = {
                    "tileset_id": tileset_id,
                    "mapbox_tileset": result['tileset_id'],
                    "created": datetime.now().isoformat(),
                    "format": result.get('format', visualization_type),
                    "source_layer": "weather_data"
                }
                
                with open(recipe_path, 'w') as f:
                    json.dump(recipe_data, f, indent=2)
                
                logger.info(f"Saved recipe info to {recipe_path}")
        else:
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = result.get('error', 'Unknown error')
                
    except Exception as e:
        logger.error(f"Error creating tileset: {str(e)}")
        if job_id in active_visualizations:
            active_visualizations[job_id]['status'] = 'failed'
            active_visualizations[job_id]['error'] = str(e)

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
        "metadata": viz_info.get('metadata'),
        "format": viz_info.get('format', 'vector'),
        "wind_components": viz_info.get('wind_components')
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
        source_layer = 'weather_data'
        
        if recipe_files:
            try:
                with open(recipe_files[0], 'r') as f:
                    recipe_data = json.load(f)
                    format_type = recipe_data.get('format', 'vector')
                    source_layer = recipe_data.get('source_layer', 'weather_data')
                    
                logger.info(f"Found recipe for {tileset_name}, format: {format_type}")
            except Exception as e:
                logger.error(f"Error reading recipe: {e}")
        
        return JSONResponse({
            "success": True,
            "tileset_id": tileset_id,
            "type": "user",
            "format": format_type,
            "config": {
                "source_layer": source_layer,
                "visualization_type": format_type
            }
        })
        
    except Exception as e:
        logger.error(f"Error loading tileset: {str(e)}")
        return JSONResponse({
            "success": False,
            "error": str(e)
        }, status_code=500)

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
                "wind_components": viz.get('wind_components')
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
    
    return {"success": True, "message": "Visualization deleted"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "mapbox_configured": bool(Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME),
        "mapbox_public_token": bool(Config.MAPBOX_PUBLIC_TOKEN),
        "active_jobs": len(active_visualizations),
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