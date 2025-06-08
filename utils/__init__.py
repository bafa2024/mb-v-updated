"""
Utils package for Weather Visualization Platform
Contains helper modules for NetCDF processing and recipe generation
"""

# This file makes the utils directory a Python package
# It can be empty, but we'll add some useful imports

from .recipe_generator import create_enhanced_recipe_for_netcdf

__all__ = ['create_enhanced_recipe_for_netcdf']

# Package metadata
__version__ = '1.0.0'
__author__ = 'Weather Visualization Platform'