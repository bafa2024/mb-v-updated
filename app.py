# app.py - Weather Visualization Application with Multi-file Upload Support and File Management
from fastapi import FastAPI, UploadFile, File, Request, Form, HTTPException, BackgroundTasks, Query
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
from typing import Optional, Dict, List, Tuple
import aiofiles
from datetime import datetime
import logging
import asyncio
from pydantic import BaseModel
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import uuid
import shutil

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
from mapbox_dataset_manager import MapboxDatasetManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(title="Weather Visualization Platform", version="5.0.0")

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
    MAX_BATCH_SIZE = int(os.getenv("MAX_BATCH_SIZE", "10"))  # Maximum files in one batch
    
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

class BatchProcessingStatus(BaseModel):
    batch_id: str
    total_files: int
    processed_files: int
    status: str
    files: List[Dict[str, any]]
    errors: List[Dict[str, str]]

class FileInfo(BaseModel):
    id: str
    filename: str
    original_filename: str
    size: int
    upload_date: str
    status: str
    metadata: Optional[Dict] = None
    tileset_id: Optional[str] = None
    job_id: Optional[str] = None
    processing_status: Optional[str] = None
    error: Optional[str] = None
    batch_id: Optional[str] = None

# In-memory storage
active_visualizations = {}
active_sessions = {}  # Store session data for client-side animation
batch_jobs = {}  # Store batch processing jobs
active_datasets = {}  # Store dataset information
uploaded_files = {}  # Store uploaded file information

# Thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=4)

# Default weather tileset
DEFAULT_TILESET = {
    "id": "mapbox.gfs-winds",
    "name": "Global Weather Data (Default)",
    "type": "default",
    "format": "raster-array"
}

# File management database (in-memory for now, can be replaced with a real database)
def load_file_database():
    """Load file information from uploads directory"""
    global uploaded_files
    uploaded_files = {}
    
    try:
        for file_path in Config.UPLOAD_DIR.glob("*.nc"):
            try:
                stat = file_path.stat()
                file_id = file_path.stem.split('_')[0]  # Extract job_id
                
                # Check if we have metadata in active_visualizations
                metadata = None
                tileset_id = None
                processing_status = "unknown"
                
                if file_id in active_visualizations:
                    viz_info = active_visualizations[file_id]
                    metadata = viz_info.get('metadata')
                    tileset_id = viz_info.get('tileset_id')
                    processing_status = viz_info.get('status', 'unknown')
                
                uploaded_files[file_id] = {
                    "id": file_id,
                    "filename": file_path.name,
                    "original_filename": '_'.join(file_path.stem.split('_')[1:]) + '.nc',
                    "size": stat.st_size,
                    "upload_date": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "status": "active",
                    "metadata": metadata,
                    "tileset_id": tileset_id,
                    "job_id": file_id,
                    "processing_status": processing_status,
                    "file_path": str(file_path)
                }
            except Exception as e:
                logger.error(f"Error loading file info for {file_path}: {e}")
                
    except Exception as e:
        logger.error(f"Error loading file database: {e}")

# Initialize file database on startup
@app.on_event("startup")
async def startup_event():
    logger.info("Starting Weather Visualization Platform v5.0...")
    logger.info(f"Mapbox Username: {Config.MAPBOX_USERNAME}")
    logger.info(f"Mapbox Token Set: {'Yes' if Config.MAPBOX_TOKEN else 'No'}")
    logger.info(f"Mapbox Public Token Set: {'Yes' if Config.MAPBOX_PUBLIC_TOKEN else 'No'}")
    logger.info(f"Max Batch Size: {Config.MAX_BATCH_SIZE}")
    
    # Load file database
    load_file_database()
    
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
                                tileset_info['format'] = recipe_data.get('actual_format', recipe_data.get('format', 'vector'))
                                tileset_info['source_layer'] = recipe_data.get('source_layer')
                                tileset_info['session_id'] = recipe_data.get('session_id')
                                tileset_info['requested_format'] = recipe_data.get('requested_format', 'vector')
                                tileset_info['use_client_animation'] = recipe_data.get('use_client_animation', False)
                                tileset_info['bounds'] = recipe_data.get('bounds')
                                tileset_info['center'] = recipe_data.get('center')
                                tileset_info['zoom'] = recipe_data.get('zoom')
                                tileset_info['batch_id'] = recipe_data.get('batch_id')  # Add batch info
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
        "default_tileset": DEFAULT_TILESET,
        "max_batch_size": Config.MAX_BATCH_SIZE
    })

# File Management API Endpoints
@app.get("/api/files")
async def list_files(
    search: Optional[str] = Query(None, description="Search term for filename"),
    status: Optional[str] = Query(None, description="Filter by status"),
    sort_by: Optional[str] = Query("upload_date", description="Sort field"),
    sort_order: Optional[str] = Query("desc", description="Sort order (asc/desc)")
):
    """List all uploaded NetCDF files with optional filtering and sorting"""
    # Reload file database to get latest info
    load_file_database()
    
    # Start with all files
    files = list(uploaded_files.values())
    
    # Apply search filter
    if search:
        search_lower = search.lower()
        files = [f for f in files if search_lower in f['original_filename'].lower()]
    
    # Apply status filter
    if status and status != "all":
        files = [f for f in files if f.get('processing_status') == status]
    
    # Sort files
    reverse = (sort_order == "desc")
    if sort_by == "filename":
        files.sort(key=lambda x: x['original_filename'], reverse=reverse)
    elif sort_by == "size":
        files.sort(key=lambda x: x['size'], reverse=reverse)
    elif sort_by == "upload_date":
        files.sort(key=lambda x: x['upload_date'], reverse=reverse)
    
    return {
        "success": True,
        "files": files,
        "total": len(files)
    }

@app.get("/api/file/{file_id}")
async def get_file_info(file_id: str):
    """Get detailed information about a specific file"""
    if file_id not in uploaded_files:
        raise HTTPException(404, "File not found")
    
    file_info = uploaded_files[file_id]
    
    # Get additional info from active visualizations if available
    if file_id in active_visualizations:
        viz_info = active_visualizations[file_id]
        file_info['visualization_info'] = {
            'tileset_id': viz_info.get('tileset_id'),
            'mapbox_tileset': viz_info.get('mapbox_tileset'),
            'format': viz_info.get('format'),
            'wind_components': viz_info.get('wind_components'),
            'bounds': viz_info.get('bounds'),
            'center': viz_info.get('center'),
            'zoom': viz_info.get('zoom')
        }
    
    return file_info

@app.delete("/api/file/{file_id}")
async def delete_file(file_id: str):
    """Delete an uploaded file and its associated data"""
    if file_id not in uploaded_files:
        raise HTTPException(404, "File not found")
    
    file_info = uploaded_files[file_id]
    file_path = Path(file_info['file_path'])
    
    try:
        # Delete the physical file
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Deleted file: {file_path}")
        
        # Remove from active visualizations
        if file_id in active_visualizations:
            del active_visualizations[file_id]
        
        # Remove from active sessions
        if file_id in active_sessions:
            del active_sessions[file_id]
        
        # Delete associated recipe files
        recipe_files = list(Config.RECIPE_DIR.glob(f"*{file_id}*.json"))
        for recipe_file in recipe_files:
            try:
                recipe_file.unlink()
                logger.info(f"Deleted recipe: {recipe_file}")
            except Exception as e:
                logger.error(f"Error deleting recipe: {e}")
        
        # Remove from uploaded files
        del uploaded_files[file_id]
        
        return {
            "success": True,
            "message": "File deleted successfully",
            "file_id": file_id
        }
        
    except Exception as e:
        logger.error(f"Error deleting file {file_id}: {e}")
        raise HTTPException(500, f"Failed to delete file: {str(e)}")

@app.post("/api/file/{file_id}/reprocess")
async def reprocess_file(
    background_tasks: BackgroundTasks,
    file_id: str,
    visualization_type: str = Form("vector")
):
    """Reprocess an existing NetCDF file"""
    if file_id not in uploaded_files:
        raise HTTPException(404, "File not found")
    
    file_info = uploaded_files[file_id]
    file_path = Path(file_info['file_path'])
    
    if not file_path.exists():
        raise HTTPException(404, "File no longer exists on disk")
    
    try:
        # Process file again
        result = await process_netcdf_file(
            file_path, file_id, True, None, visualization_type
        )
        
        if result.get('wind_data'):
            active_sessions[file_id] = {
                'file_path': str(file_path),
                'wind_data': result['wind_data'],
                'bounds': result.get('bounds'),
                'center': result.get('center'),
                'zoom': result.get('zoom'),
                'created_at': datetime.now().isoformat()
            }
            result['session_id'] = file_id
        
        if Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME:
            # Start background tileset creation
            background_tasks.add_task(
                create_mapbox_tileset_background,
                file_path,
                file_id,
                result.get('tileset_id'),
                visualization_type
            )
            
            result['status'] = 'processing'
            result['message'] = f'Reprocessing file {file_info["original_filename"]}...'
        
        # Update file info
        uploaded_files[file_id]['processing_status'] = 'processing'
        uploaded_files[file_id]['metadata'] = result.get('metadata')
        
        return result
        
    except Exception as e:
        logger.error(f"Error reprocessing file {file_id}: {e}")
        raise HTTPException(500, f"Failed to reprocess file: {str(e)}")

@app.get("/api/file/{file_id}/download")
async def download_file(file_id: str):
    """Download the original NetCDF file"""
    if file_id not in uploaded_files:
        raise HTTPException(404, "File not found")
    
    file_info = uploaded_files[file_id]
    file_path = Path(file_info['file_path'])
    
    if not file_path.exists():
        raise HTTPException(404, "File no longer exists on disk")
    
    return FileResponse(
        path=str(file_path),
        filename=file_info['original_filename'],
        media_type='application/x-netcdf'
    )

@app.post("/api/files/delete-batch")
async def delete_files_batch(file_ids: List[str]):
    """Delete multiple files at once"""
    deleted = []
    errors = []
    
    for file_id in file_ids:
        try:
            if file_id in uploaded_files:
                # Use the existing delete logic
                file_info = uploaded_files[file_id]
                file_path = Path(file_info['file_path'])
                
                # Delete the physical file
                if file_path.exists():
                    file_path.unlink()
                
                # Clean up associated data
                if file_id in active_visualizations:
                    del active_visualizations[file_id]
                if file_id in active_sessions:
                    del active_sessions[file_id]
                
                # Delete recipe files
                recipe_files = list(Config.RECIPE_DIR.glob(f"*{file_id}*.json"))
                for recipe_file in recipe_files:
                    try:
                        recipe_file.unlink()
                    except:
                        pass
                
                del uploaded_files[file_id]
                deleted.append(file_id)
            else:
                errors.append({"file_id": file_id, "error": "File not found"})
                
        except Exception as e:
            errors.append({"file_id": file_id, "error": str(e)})
    
    return {
        "success": len(deleted) > 0,
        "deleted": deleted,
        "errors": errors,
        "message": f"Deleted {len(deleted)} files, {len(errors)} errors"
    }

# Existing endpoints remain the same...
@app.post("/api/upload-netcdf")
async def upload_netcdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    create_tileset: bool = Form(True),
    tileset_name: Optional[str] = Form(None),
    visualization_type: str = Form("vector")
):
    """Upload and process single NetCDF file (backward compatibility)"""
    
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
    
    # Process single file using the same logic as batch with one file
    files = [{"file": file, "content": content}]
    result = await process_batch_upload(
        files=files,
        job_ids=[job_id],
        create_tileset=create_tileset,
        tileset_names=[tileset_name] if tileset_name else None,
        visualization_type=visualization_type,
        background_tasks=background_tasks
    )
    
    # Update file database
    if result['files']:
        file_result = result['files'][0]
        if file_result.get('success'):
            uploaded_files[job_id] = {
                "id": job_id,
                "filename": f"{job_id}_{file.filename}",
                "original_filename": file.filename,
                "size": file_size,
                "upload_date": datetime.now().isoformat(),
                "status": "active",
                "metadata": file_result.get('metadata'),
                "tileset_id": file_result.get('tileset_id'),
                "job_id": job_id,
                "processing_status": file_result.get('status', 'processing'),
                "file_path": str(Config.UPLOAD_DIR / f"{job_id}_{file.filename}")
            }
    
    # Return single file result
    if result['files']:
        return JSONResponse(result['files'][0])
    else:
        return JSONResponse({
            "success": False,
            "error": "Failed to process file"
        }, status_code=500)

@app.post("/api/upload-netcdf-batch")
async def upload_netcdf_batch(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    create_tileset: bool = Form(True),
    tileset_names: Optional[str] = Form(None),  # Comma-separated names
    visualization_type: str = Form("vector"),
    merge_files: bool = Form(False)  # Option to merge files into single tileset
):
    """Upload and process multiple NetCDF files"""
    
    # Validate batch size
    if len(files) > Config.MAX_BATCH_SIZE:
        raise HTTPException(400, f"Too many files. Maximum batch size is {Config.MAX_BATCH_SIZE}")
    
    # Validate all files
    for file in files:
        if not file.filename.endswith('.nc'):
            raise HTTPException(400, f"Invalid file type: {file.filename}. Only NetCDF (.nc) files are allowed")
    
    # Create batch ID
    batch_id = str(uuid.uuid4())
    
    # Parse tileset names
    tileset_name_list = None
    if tileset_names:
        tileset_name_list = [name.strip() for name in tileset_names.split(',')]
        if len(tileset_name_list) != len(files):
            tileset_name_list = None  # Ignore if count doesn't match
    
    # Read all files
    file_contents = []
    job_ids = []
    
    for i, file in enumerate(files):
        content = await file.read()
        
        # Check individual file size
        if len(content) > Config.MAX_FILE_SIZE:
            raise HTTPException(400, f"File {file.filename} too large. Maximum size is {Config.MAX_FILE_SIZE / 1024 / 1024}MB")
        
        job_id = f"{batch_id}_{i}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        job_ids.append(job_id)
        file_contents.append({"file": file, "content": content})
    
    # Initialize batch job
    batch_jobs[batch_id] = {
        "batch_id": batch_id,
        "total_files": len(files),
        "processed_files": 0,
        "status": "processing",
        "files": [],
        "errors": [],
        "created_at": datetime.now().isoformat()
    }
    
    # Process files
    if merge_files:
        # TODO: Implement file merging logic
        # For now, process separately
        result = await process_batch_upload(
            files=file_contents,
            job_ids=job_ids,
            create_tileset=create_tileset,
            tileset_names=tileset_name_list,
            visualization_type=visualization_type,
            background_tasks=background_tasks,
            batch_id=batch_id
        )
    else:
        # Process files separately
        result = await process_batch_upload(
            files=file_contents,
            job_ids=job_ids,
            create_tileset=create_tileset,
            tileset_names=tileset_name_list,
            visualization_type=visualization_type,
            background_tasks=background_tasks,
            batch_id=batch_id
        )
    
    # Update batch job status
    batch_jobs[batch_id].update(result)
    
    # Update file database for each file
    for i, file_result in enumerate(result.get('files', [])):
        if file_result.get('success'):
            job_id = job_ids[i]
            file = files[i]
            uploaded_files[job_id] = {
                "id": job_id,
                "filename": f"{job_id}_{file.filename}",
                "original_filename": file.filename,
                "size": len(file_contents[i]['content']),
                "upload_date": datetime.now().isoformat(),
                "status": "active",
                "metadata": file_result.get('metadata'),
                "tileset_id": file_result.get('tileset_id'),
                "job_id": job_id,
                "processing_status": file_result.get('status', 'processing'),
                "batch_id": batch_id,
                "file_path": str(Config.UPLOAD_DIR / f"{job_id}_{file.filename}")
            }
    
    return JSONResponse(result)

async def process_batch_upload(
    files: List[Dict],
    job_ids: List[str],
    create_tileset: bool,
    tileset_names: Optional[List[str]],
    visualization_type: str,
    background_tasks: BackgroundTasks,
    batch_id: Optional[str] = None
) -> Dict:
    """Process multiple NetCDF files"""
    
    results = {
        "batch_id": batch_id,
        "total_files": len(files),
        "processed_files": 0,
        "status": "processing",
        "files": [],
        "errors": []
    }
    
    # Process each file
    for i, file_data in enumerate(files):
        file = file_data['file']
        content = file_data['content']
        job_id = job_ids[i]
        tileset_name = tileset_names[i] if tileset_names and i < len(tileset_names) else None
        
        try:
            # Sanitize filename
            safe_filename = Path(file.filename).name
            safe_filename = ''.join(c if c.isalnum() or c in '.-_' else '_' for c in safe_filename)
            if not safe_filename.endswith('.nc'):
                safe_filename = safe_filename.rsplit('.', 1)[0] + '.nc'
            
            # Save uploaded file
            file_path = Config.UPLOAD_DIR / f"{job_id}_{safe_filename}"
            
            logger.info(f"Saving uploaded file: {file_path}")
            
            async with aiofiles.open(str(file_path), 'wb') as f:
                await f.write(content)
            
            # Process file
            result = await process_netcdf_file(
                file_path, job_id, create_tileset, tileset_name, visualization_type, batch_id
            )
            
            # Store session data for client-side animation
            if result.get('wind_data'):
                active_sessions[job_id] = {
                    'file_path': str(file_path),
                    'wind_data': result['wind_data'],
                    'bounds': result.get('bounds'),
                    'center': result.get('center'),
                    'zoom': result.get('zoom'),
                    'created_at': datetime.now().isoformat(),
                    'batch_id': batch_id
                }
                result['session_id'] = job_id
            
            if create_tileset and Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME:
                # Start background tileset creation
                background_tasks.add_task(
                    create_mapbox_tileset_background,
                    file_path,
                    job_id,
                    result.get('tileset_id'),
                    visualization_type,
                    batch_id
                )
                
                result['status'] = 'processing'
                result['message'] = f'File {file.filename} uploaded successfully. Creating Mapbox tileset...'
            
            results['files'].append({
                "filename": file.filename,
                "job_id": job_id,
                "success": True,
                **result
            })
            results['processed_files'] += 1
            
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            logger.error(traceback.format_exc())
            
            results['errors'].append({
                "filename": file.filename,
                "error": str(e)
            })
            
            # Clean up file on error
            if 'file_path' in locals() and file_path.exists():
                try:
                    file_path.unlink()
                except:
                    pass
    
    # Update overall status
    if results['processed_files'] == results['total_files']:
        results['status'] = 'completed'
    elif results['processed_files'] > 0:
        results['status'] = 'partial'
    else:
        results['status'] = 'failed'
    
    return results

async def process_netcdf_file(file_path: Path, job_id: str, create_tileset: bool, 
                             tileset_name: Optional[str], visualization_type: str,
                             batch_id: Optional[str] = None) -> Dict:
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
        
        # Calculate optimal center and zoom for the data region
        center, zoom = calculate_optimal_view(bounds) if bounds else (None, None)
        
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
        
        # Add batch indicator if part of batch
        if batch_id:
            prefix = f"wxb_{batch_id[:8]}"
        
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
            "center": center,
            "zoom": zoom,
            "visualization_type": visualization_type,
            "requested_format": "raster-array" if visualization_type == "raster-array" else "vector",
            "created_at": datetime.now().isoformat(),
            "status": "processing",
            "scalar_vars": scalar_vars,
            "vector_pairs": vector_pairs,
            "session_id": job_id,
            "batch_id": batch_id
        }
        
        ds.close()
        
        return {
            "success": True,
            "job_id": job_id,
            "tileset_id": tileset_id,
            "metadata": metadata,
            "wind_components": wind_components,
            "bounds": bounds,
            "center": center,
            "zoom": zoom,
            "visualization_type": visualization_type,
            "requested_format": "raster-array" if visualization_type == "raster-array" else "vector",
            "scalar_vars": scalar_vars,
            "vector_pairs": vector_pairs,
            "previews": previews,
            "wind_data": wind_data,
            "session_id": job_id,
            "batch_id": batch_id
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

def calculate_optimal_view(bounds: Dict) -> tuple:
    """Calculate optimal center point and zoom level for given bounds"""
    if not bounds:
        return None, None
    
    # Calculate center
    center_lon = (bounds['east'] + bounds['west']) / 2
    center_lat = (bounds['north'] + bounds['south']) / 2
    
    # Calculate zoom level based on bounds
    lat_diff = bounds['north'] - bounds['south']
    lon_diff = bounds['east'] - bounds['west']
    
    # Use the larger dimension to calculate zoom
    max_diff = max(lat_diff, lon_diff)
    
    # Approximate zoom calculation (simplified)
    # These values work well for most cases
    if max_diff > 180:
        zoom = 1
    elif max_diff > 90:
        zoom = 2
    elif max_diff > 45:
        zoom = 3
    elif max_diff > 22:
        zoom = 4
    elif max_diff > 11:
        zoom = 5
    elif max_diff > 5.5:
        zoom = 6
    elif max_diff > 2.8:
        zoom = 7
    elif max_diff > 1.4:
        zoom = 8
    else:
        zoom = 9
    
    return [center_lon, center_lat], zoom

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
                                          tileset_id: str, visualization_type: str,
                                          batch_id: Optional[str] = None):
    """Background task to create Mapbox tileset with proper error handling"""
    try:
        if not Config.MAPBOX_TOKEN:
            logger.error("Mapbox token not configured")
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = 'Mapbox token not configured'
            # Update file database
            if job_id in uploaded_files:
                uploaded_files[job_id]['processing_status'] = 'failed'
                uploaded_files[job_id]['error'] = 'Mapbox token not configured'
            return
        
        # Convert Path to string
        file_path_str = str(file_path)
        
        # Verify file exists
        if not os.path.exists(file_path_str):
            logger.error(f"NetCDF file not found: {file_path_str}")
            if job_id in active_visualizations:
                active_visualizations[job_id]['status'] = 'failed'
                active_visualizations[job_id]['error'] = 'Input file not found'
            # Update file database
            if job_id in uploaded_files:
                uploaded_files[job_id]['processing_status'] = 'failed'
                uploaded_files[job_id]['error'] = 'Input file not found'
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
                
                # Update file database
                if job_id in uploaded_files:
                    uploaded_files[job_id]['processing_status'] = 'completed'
                    uploaded_files[job_id]['tileset_id'] = result['tileset_id']
                    
                logger.info("Successfully created raster-array tileset")
                
                # Update batch job if part of batch
                if batch_id and batch_id in batch_jobs:
                    for file_info in batch_jobs[batch_id]['files']:
                        if file_info.get('job_id') == job_id:
                            file_info['status'] = 'completed'
                            break
                
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
                    
                    # Update file database
                    if job_id in uploaded_files:
                        uploaded_files[job_id]['processing_status'] = 'failed'
                        uploaded_files[job_id]['error'] = result.get('error')
                    
                    # Update batch job if part of batch
                    if batch_id and batch_id in batch_jobs:
                        for file_info in batch_jobs[batch_id]['files']:
                            if file_info.get('job_id') == job_id:
                                file_info['status'] = 'failed'
                                file_info['error'] = result.get('error')
                                break
                    
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
                
                # Update file database
                if job_id in uploaded_files:
                    uploaded_files[job_id]['processing_status'] = 'completed'
                    uploaded_files[job_id]['tileset_id'] = result['tileset_id']
                
                # Update batch job if part of batch
                if batch_id and batch_id in batch_jobs:
                    for file_info in batch_jobs[batch_id]['files']:
                        if file_info.get('job_id') == job_id:
                            file_info['status'] = 'completed'
                            break
                        
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Tileset creation failed: {error_msg}")
                
                if job_id in active_visualizations:
                    active_visualizations[job_id]['status'] = 'failed'
                    active_visualizations[job_id]['error'] = error_msg
                
                # Update file database
                if job_id in uploaded_files:
                    uploaded_files[job_id]['processing_status'] = 'failed'
                    uploaded_files[job_id]['error'] = error_msg
                
                # Update batch job if part of batch
                if batch_id and batch_id in batch_jobs:
                    for file_info in batch_jobs[batch_id]['files']:
                        if file_info.get('job_id') == job_id:
                            file_info['status'] = 'failed'
                            file_info['error'] = error_msg
                            break
                
    except Exception as e:
        logger.error(f"Error creating tileset: {str(e)}")
        import traceback
        traceback.print_exc()
        
        if job_id in active_visualizations:
            active_visualizations[job_id]['status'] = 'failed'
            active_visualizations[job_id]['error'] = str(e)
        
        # Update file database
        if job_id in uploaded_files:
            uploaded_files[job_id]['processing_status'] = 'failed'
            uploaded_files[job_id]['error'] = str(e)
        
        # Update batch job if part of batch
        if batch_id and batch_id in batch_jobs:
            for file_info in batch_jobs[batch_id]['files']:
                if file_info.get('job_id') == job_id:
                    file_info['status'] = 'failed'
                    file_info['error'] = str(e)
                    break

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
        "format": actual_format,
        "actual_format": actual_format,
        "requested_format": viz_info.get('requested_format', 'vector'),
        "source_layer": result.get('source_layer', 'weather_data' if actual_format == 'vector' else '10winds'),
        "recipe_id": result.get('recipe_id'),
        "publish_job_id": result.get('publish_job_id'),
        "scalar_vars": viz_info.get("scalar_vars", []),
        "vector_pairs": viz_info.get("vector_pairs", []),
        "visualization_type": viz_info.get('visualization_type', 'vector'),
        "is_raster_array": actual_format == 'raster-array',
        "use_client_animation": viz_info.get('use_client_animation', False),
        "session_id": viz_info.get('session_id'),
        "bounds": viz_info.get('bounds'),  # Save bounds
        "center": viz_info.get('center'),  # Save center
        "zoom": viz_info.get('zoom'),      # Save zoom
        "batch_id": viz_info.get('batch_id')  # Save batch info
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
    
    # Update file database status if needed
    if job_id in uploaded_files:
        uploaded_files[job_id]['processing_status'] = viz_info.get('status', 'processing')
        if viz_info.get('error'):
            uploaded_files[job_id]['error'] = viz_info.get('error')
    
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
        "session_id": viz_info.get('session_id'),
        "bounds": viz_info.get('bounds'),
        "center": viz_info.get('center'),
        "zoom": viz_info.get('zoom'),
        "batch_id": viz_info.get('batch_id')
    })

@app.get("/api/batch-status/{batch_id}")
async def get_batch_status(batch_id: str):
    """Get status of batch processing"""
    if batch_id not in batch_jobs:
        raise HTTPException(404, "Batch job not found")
    
    batch_info = batch_jobs[batch_id]
    
    # Check individual file statuses
    completed = 0
    failed = 0
    processing = 0
    
    for file_info in batch_info['files']:
        job_id = file_info.get('job_id')
        if job_id in active_visualizations:
            status = active_visualizations[job_id].get('status', 'processing')
            if status == 'completed':
                completed += 1
            elif status == 'failed':
                failed += 1
            else:
                processing += 1
    
    # Update batch status
    if processing > 0:
        batch_info['status'] = 'processing'
    elif failed == len(batch_info['files']):
        batch_info['status'] = 'failed'
    elif completed == len(batch_info['files']):
        batch_info['status'] = 'completed'
    else:
        batch_info['status'] = 'partial'
    
    batch_info['completed_files'] = completed
    batch_info['failed_files'] = failed
    batch_info['processing_files'] = processing
    
    return JSONResponse(batch_info)

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
        bounds = None
        center = None
        zoom = None
        batch_id = None
        
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
                    bounds = recipe_data.get('bounds')
                    center = recipe_data.get('center')
                    zoom = recipe_data.get('zoom')
                    batch_id = recipe_data.get('batch_id')
                    
                    # Double-check format based on source layer
                    if source_layer == '10winds' or is_raster_array:
                        actual_format = 'raster-array'
                    
                logger.info(f"Found recipe for {tileset_name}, format: {format_type}, actual: {actual_format}, requested: {requested_format}")
            except Exception as e:
                logger.error(f"Error reading recipe: {e}")
        
        # Check if tileset exists on Mapbox and verify its type
        if Config.MAPBOX_TOKEN:
            manager = MapboxTilesetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
            tileset_info = manager.check_tileset_format(tileset_id)
            
            if tileset_info.get('success'):
                # Use the format information from Mapbox
                actual_format = tileset_info.get('format', actual_format)
                if actual_format == 'raster-array':
                    source_layer = '10winds'
                else:
                    source_layer = 'weather_data'
                    
                logger.info(f"Mapbox confirms tileset format: {actual_format}")
        
        return JSONResponse({
            "success": True,
            "tileset_id": tileset_id,
            "type": "user",
            "format": actual_format,
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
                "session_id": session_id,
                "bounds": bounds,
                "center": center,
                "zoom": zoom,
                "batch_id": batch_id
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

@app.post("/api/upload-netcdf-as-dataset")
async def upload_netcdf_as_dataset(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    dataset_name: Optional[str] = Form(None)
):
    """Upload NetCDF file and create a Mapbox dataset"""
    
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
    
    # Save file temporarily
    safe_filename = Path(file.filename).name
    safe_filename = ''.join(c if c.isalnum() or c in '.-_' else '_' for c in safe_filename)
    file_path = Config.UPLOAD_DIR / f"{job_id}_{safe_filename}"
    
    try:
        async with aiofiles.open(str(file_path), 'wb') as f:
            await f.write(content)
        
        # Process in background
        background_tasks.add_task(
            create_dataset_background,
            file_path,
            job_id,
            dataset_name,
            file.filename
        )
        
        return JSONResponse({
            "success": True,
            "job_id": job_id,
            "message": "File uploaded. Creating dataset...",
            "status": "processing"
        })
        
    except Exception as e:
        logger.error(f"Error uploading file for dataset: {str(e)}")
        if file_path.exists():
            file_path.unlink()
        raise HTTPException(500, str(e))

@app.post("/api/upload-netcdf-batch-as-datasets")
async def upload_netcdf_batch_as_datasets(
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    dataset_names: Optional[str] = Form(None)  # Comma-separated names
):
    """Upload multiple NetCDF files and create Mapbox datasets"""
    
    # Validate batch size
    if len(files) > Config.MAX_BATCH_SIZE:
        raise HTTPException(400, f"Too many files. Maximum batch size is {Config.MAX_BATCH_SIZE}")
    
    # Validate all files
    for file in files:
        if not file.filename.endswith('.nc'):
            raise HTTPException(400, f"Invalid file type: {file.filename}. Only NetCDF (.nc) files are allowed")
    
    # Create batch ID
    batch_id = str(uuid.uuid4())
    
    # Parse dataset names
    dataset_name_list = None
    if dataset_names:
        dataset_name_list = [name.strip() for name in dataset_names.split(',')]
        if len(dataset_name_list) != len(files):
            dataset_name_list = None
    
    # Initialize batch job
    batch_jobs[batch_id] = {
        "batch_id": batch_id,
        "type": "dataset",
        "total_files": len(files),
        "processed_files": 0,
        "status": "processing",
        "files": [],
        "errors": [],
        "datasets": [],
        "created_at": datetime.now().isoformat()
    }
    
    # Process each file
    for i, file in enumerate(files):
        content = await file.read()
        
        # Check file size
        if len(content) > Config.MAX_FILE_SIZE:
            batch_jobs[batch_id]['errors'].append({
                "filename": file.filename,
                "error": f"File too large. Maximum size is {Config.MAX_FILE_SIZE / 1024 / 1024}MB"
            })
            continue
        
        job_id = f"{batch_id}_{i}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Save file
        safe_filename = Path(file.filename).name
        safe_filename = ''.join(c if c.isalnum() or c in '.-_' else '_' for c in safe_filename)
        file_path = Config.UPLOAD_DIR / f"{job_id}_{safe_filename}"
        
        try:
            async with aiofiles.open(str(file_path), 'wb') as f:
                await f.write(content)
            
            dataset_name = None
            if dataset_name_list and i < len(dataset_name_list):
                dataset_name = dataset_name_list[i]
            
            # Process in background
            background_tasks.add_task(
                create_dataset_background,
                file_path,
                job_id,
                dataset_name,
                file.filename,
                batch_id
            )
            
            batch_jobs[batch_id]['files'].append({
                "filename": file.filename,
                "job_id": job_id,
                "status": "processing"
            })
            
        except Exception as e:
            logger.error(f"Error processing file {file.filename}: {str(e)}")
            batch_jobs[batch_id]['errors'].append({
                "filename": file.filename,
                "error": str(e)
            })
            if file_path.exists():
                file_path.unlink()
    
    return JSONResponse(batch_jobs[batch_id])

async def create_dataset_background(
    file_path: Path,
    job_id: str,
    dataset_name: Optional[str],
    original_filename: str,
    batch_id: Optional[str] = None
):
    """Background task to create Mapbox dataset from NetCDF"""
    try:
        if not Config.MAPBOX_TOKEN:
            raise Exception("Mapbox token not configured")
        
        # Initialize dataset manager
        dataset_manager = MapboxDatasetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
        
        # Create dataset from NetCDF
        logger.info(f"Creating dataset from {file_path}")
        
        if not dataset_name:
            dataset_name = f"Weather Data - {Path(original_filename).stem}"
        
        result = dataset_manager.process_netcdf_to_dataset(str(file_path), dataset_name)
        
        # Store dataset info
        if result['success']:
            active_datasets[job_id] = {
                "job_id": job_id,
                "dataset_id": result['dataset_id'],
                "dataset_url": result.get('dataset_url'),
                "filename": original_filename,
                "total_features": result.get('total_features', 0),
                "features_added": result.get('features_added', 0),
                "status": "completed",
                "created_at": datetime.now().isoformat(),
                "batch_id": batch_id
            }
            
            # Update batch job if part of batch
            if batch_id and batch_id in batch_jobs:
                batch_jobs[batch_id]['datasets'].append({
                    "dataset_id": result['dataset_id'],
                    "dataset_url": result.get('dataset_url'),
                    "filename": original_filename,
                    "features": result.get('features_added', 0)
                })
                
                # Update file status
                for file_info in batch_jobs[batch_id]['files']:
                    if file_info.get('job_id') == job_id:
                        file_info['status'] = 'completed'
                        file_info['dataset_id'] = result['dataset_id']
                        break
                
                batch_jobs[batch_id]['processed_files'] += 1
                
                # Update batch status
                if batch_jobs[batch_id]['processed_files'] == batch_jobs[batch_id]['total_files']:
                    batch_jobs[batch_id]['status'] = 'completed'
                elif batch_jobs[batch_id]['processed_files'] > 0:
                    batch_jobs[batch_id]['status'] = 'partial'
            
            logger.info(f"Successfully created dataset: {result['dataset_id']}")
            
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"Failed to create dataset: {error_msg}")
            
            active_datasets[job_id] = {
                "job_id": job_id,
                "filename": original_filename,
                "status": "failed",
                "error": error_msg,
                "created_at": datetime.now().isoformat(),
                "batch_id": batch_id
            }
            
            # Update batch job if part of batch
            if batch_id and batch_id in batch_jobs:
                for file_info in batch_jobs[batch_id]['files']:
                    if file_info.get('job_id') == job_id:
                        file_info['status'] = 'failed'
                        file_info['error'] = error_msg
                        break
                
                batch_jobs[batch_id]['processed_files'] += 1
                
                if batch_jobs[batch_id]['processed_files'] == batch_jobs[batch_id]['total_files']:
                    if all(f.get('status') == 'failed' for f in batch_jobs[batch_id]['files']):
                        batch_jobs[batch_id]['status'] = 'failed'
                    else:
                        batch_jobs[batch_id]['status'] = 'partial'
        
    except Exception as e:
        logger.error(f"Error creating dataset: {str(e)}")
        import traceback
        traceback.print_exc()
        
        active_datasets[job_id] = {
            "job_id": job_id,
            "filename": original_filename,
            "status": "failed",
            "error": str(e),
            "created_at": datetime.now().isoformat(),
            "batch_id": batch_id
        }
        
        # Update batch job if part of batch
        if batch_id and batch_id in batch_jobs:
            for file_info in batch_jobs[batch_id]['files']:
                if file_info.get('job_id') == job_id:
                    file_info['status'] = 'failed'
                    file_info['error'] = str(e)
                    break
            
            batch_jobs[batch_id]['processed_files'] += 1
    
    finally:
        # Clean up file
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            logger.error(f"Error cleaning up file: {e}")

@app.get("/api/dataset-status/{job_id}")
async def get_dataset_status(job_id: str):
    """Get status of dataset creation"""
    if job_id not in active_datasets:
        raise HTTPException(404, "Job not found")
    
    return JSONResponse(active_datasets[job_id])

@app.get("/api/list-datasets")
async def list_datasets():
    """List all user's datasets"""
    if not Config.MAPBOX_TOKEN:
        raise HTTPException(500, "Mapbox token not configured")
    
    try:
        dataset_manager = MapboxDatasetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
        datasets = dataset_manager.list_datasets(limit=100)
        
        # Add weather data indicator
        weather_datasets = []
        for ds in datasets:
            # Include weather-related datasets
            dataset_name = ds.get('name', '').lower()
            dataset_id = ds.get('id', '')
            
            if any(keyword in dataset_name or keyword in dataset_id.lower() 
                  for keyword in ['weather', 'netcdf', 'wind', 'temperature', 'pressure']):
                weather_datasets.append(ds)
        
        return JSONResponse({
            "success": True,
            "total_datasets": len(datasets),
            "weather_datasets": weather_datasets,
            "all_datasets": datasets
        })
        
    except Exception as e:
        logger.error(f"Error listing datasets: {str(e)}")
        raise HTTPException(500, str(e))

@app.get("/api/dataset-info/{dataset_id}")
async def get_dataset_info(dataset_id: str):
    """Get detailed information about a dataset"""
    if not Config.MAPBOX_TOKEN:
        raise HTTPException(500, "Mapbox token not configured")
    
    try:
        dataset_manager = MapboxDatasetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
        info = dataset_manager.get_dataset_info(dataset_id)
        
        if 'error' in info:
            raise HTTPException(404, info['error'])
        
        return JSONResponse(info)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting dataset info: {str(e)}")
        raise HTTPException(500, str(e))

@app.delete("/api/dataset/{dataset_id}")
async def delete_dataset(dataset_id: str):
    """Delete a dataset"""
    if not Config.MAPBOX_TOKEN:
        raise HTTPException(500, "Mapbox token not configured")
    
    try:
        dataset_manager = MapboxDatasetManager(Config.MAPBOX_TOKEN, Config.MAPBOX_USERNAME)
        success = dataset_manager.delete_dataset(dataset_id)
        
        if success:
            # Remove from active datasets if exists
            for job_id, ds_info in list(active_datasets.items()):
                if ds_info.get('dataset_id') == dataset_id:
                    del active_datasets[job_id]
            
            return {"success": True, "message": "Dataset deleted successfully"}
        else:
            raise HTTPException(400, "Failed to delete dataset")
            
    except Exception as e:
        logger.error(f"Error deleting dataset: {str(e)}")
        raise HTTPException(500, str(e))

@app.get("/api/active-datasets")
async def get_active_datasets():
    """Get list of recently created datasets"""
    return JSONResponse({
        "datasets": list(active_datasets.values()),
        "total": len(active_datasets)
    })

@app.post("/api/dataset-to-tileset/{dataset_id}")
async def convert_dataset_to_tileset(
    dataset_id: str,
    tileset_name: Optional[str] = Form(None)
):
    """Convert a dataset to a tileset for visualization"""
    if not Config.MAPBOX_TOKEN:
        raise HTTPException(500, "Mapbox token not configured")
    
    try:
        # This would typically involve:
        # 1. Exporting dataset as GeoJSON
        # 2. Creating a tileset source
        # 3. Creating and publishing a tileset
        
        # For now, return a placeholder
        return JSONResponse({
            "success": False,
            "message": "Dataset to tileset conversion requires additional implementation",
            "info": "You can export the dataset from Mapbox Studio and then upload as a tileset"
        })
        
    except Exception as e:
        logger.error(f"Error converting dataset to tileset: {str(e)}")
        raise HTTPException(500, str(e))

@app.get("/api/active-visualizations")
async def get_active_visualizations():
    """Get list of active visualizations"""
    # Group by batch if applicable
    batched_visualizations = {}
    single_visualizations = []
    
    for job_id, viz in active_visualizations.items():
        batch_id = viz.get('batch_id')
        if batch_id:
            if batch_id not in batched_visualizations:
                batched_visualizations[batch_id] = []
            batched_visualizations[batch_id].append({
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
                "session_id": viz.get('session_id'),
                "bounds": viz.get('bounds'),
                "center": viz.get('center'),
                "zoom": viz.get('zoom')
            })
        else:
            single_visualizations.append({
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
                "session_id": viz.get('session_id'),
                "bounds": viz.get('bounds'),
                "center": viz.get('center'),
                "zoom": viz.get('zoom')
            })
    
    return JSONResponse({
        "single_visualizations": single_visualizations,
        "batched_visualizations": batched_visualizations,
        "batch_jobs": batch_jobs
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
    
    # Remove from uploaded files
    if job_id in uploaded_files:
        del uploaded_files[job_id]
    
    return {"success": True, "message": "Visualization deleted"}

@app.delete("/api/batch/{batch_id}")
async def delete_batch(batch_id: str):
    """Delete all visualizations in a batch"""
    if batch_id not in batch_jobs:
        raise HTTPException(404, "Batch not found")
    
    batch_info = batch_jobs[batch_id]
    deleted_count = 0
    
    # Delete all visualizations in the batch
    for file_info in batch_info['files']:
        job_id = file_info.get('job_id')
        if job_id and job_id in active_visualizations:
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
            
            # Remove from uploaded files
            if job_id in uploaded_files:
                del uploaded_files[job_id]
            
            deleted_count += 1
    
    # Remove batch job
    del batch_jobs[batch_id]
    
    return {
        "success": True,
        "message": f"Batch deleted. Removed {deleted_count} visualizations.",
        "deleted_count": deleted_count
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "mapbox_configured": bool(Config.MAPBOX_TOKEN and Config.MAPBOX_USERNAME),
        "mapbox_public_token": bool(Config.MAPBOX_PUBLIC_TOKEN),
        "active_jobs": len(active_visualizations),
        "active_sessions": len(active_sessions),
        "active_batches": len(batch_jobs),
        "active_datasets": len(active_datasets),
        "uploaded_files": len(uploaded_files),
        "version": "5.0.0"
    }

# Cleanup old files periodically
async def cleanup_old_files():
    """Remove old temporary files"""
    try:
        cutoff_time = datetime.now().timestamp() - (24 * 3600)  # 24 hours
        
        for dir_path in [Config.UPLOAD_DIR, Config.PROCESSED_DIR]:
            for file_path in dir_path.glob("*"):
                if file_path.is_file() and file_path.stat().st_mtime < cutoff_time:
                    # Check if file is still in use
                    file_id = file_path.stem.split('_')[0]
                    if file_id not in uploaded_files and file_id not in active_visualizations:
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
        
        # Clean up old batch jobs
        to_remove = []
        for batch_id, batch_data in batch_jobs.items():
            created_at = datetime.fromisoformat(batch_data.get('created_at', datetime.now().isoformat()))
            if (datetime.now() - created_at).total_seconds() > 24 * 3600:
                to_remove.append(batch_id)
        
        for batch_id in to_remove:
            del batch_jobs[batch_id]
            logger.info(f"Cleaned up old batch job: {batch_id}")
                    
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down Weather Visualization Platform...")
    executor.shutdown(wait=True)

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