"""
Simple Recipe Generator for Mapbox Tilesets
Creates minimal, valid recipes for weather data
"""

import logging

logger = logging.getLogger(__name__)


def create_simple_recipe(source_id: str, username: str) -> dict:
    """
    Create a minimal valid recipe for Mapbox tileset
    
    Args:
        source_id: The tileset source ID
        username: Mapbox username
        
    Returns:
        A minimal recipe dictionary
    """
    
    recipe = {
        "version": 1,
        "layers": {
            "weather_data": {
                "source": f"mapbox://tileset-source/{username}/{source_id}",
                "minzoom": 0,
                "maxzoom": 5,
                "features": {
                    "attributes": {
                        "allowed_output": [
                            "u10",
                            "v10", 
                            "speed",
                            "direction",
                            "lat",
                            "lon"
                        ]
                    }
                }
            }
        }
    }
    
    return recipe


def create_recipe_with_zoom_levels(source_id: str, username: str) -> dict:
    """
    Create a recipe with different zoom level configurations
    """
    
    recipe = {
        "version": 1,
        "layers": {
            "weather_overview": {
                "source": f"mapbox://tileset-source/{username}/{source_id}",
                "minzoom": 0,
                "maxzoom": 3,
                "features": {
                    "simplification": {
                        "distance": 10,
                        "zoom": 0
                    }
                }
            },
            "weather_detail": {
                "source": f"mapbox://tileset-source/{username}/{source_id}",
                "minzoom": 3,
                "maxzoom": 5
            }
        }
    }
    
    return recipe