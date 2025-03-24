#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 21 16:52:39 2025

@author: haze
"""
import cProfile
import logging
import os
from datetime import datetime
from itertools import product
import xarray as xr
import numpy as np
import pandas as pd
import scipy
import yaml
# from dask import compute, delayed
# from dask.diagnostics import ProgressBar
from dask.distributed import as_completed, progress
from dask_jobqueue import SLURMCluster
from distributed import Client, LocalCluster
# import dask
from tornado import gen

from cosipy.config import Config, SlurmConfig
from cosipy.constants import Constants
from cosipy.cpkernel.cosipy_core import cosipy_core
from cosipy.cpkernel.io import IOClass
from oggm import cfg, utils
from oggm.utils import floatyear_to_date
from oggm.cfg import G, SEC_IN_YEAR, SEC_IN_DAY
cfg.initialize(logging_level='WARNING')



class cosipymb():
    
    def __init__(self,heights = None ,year = None , DATA = None, date = None, RESTART=None):
        self.heights = heights
        self.year = year
        self.year = date
        self.date = utils.floatyear_to_date(year)
        self.year = date[0]
        self.month = date[1]
        DATAsel = self.DATA.isel(time = (self.xopen.time.dt.year == year))
        self.DATA = DATAsel.isel(month=(month-1))
        self.RESTART = RESTART
        
        #here i want to get heights and the step date (doing monthly for the time being)
        #heights from OGGM need to have dims- use create_static_file to make the DEMs for COSIPY
  #Then put heights and ice_thick into here to then get a new glacier mask by doing
  #DATA.MASK = DATA.HGT.where(heights+ice_thk-heights !=0,1)
  #timestep is just slice- something like this 
 # self.year = date
 # date = utils.floatyear_to_date(year)
 # year = date[0]
 # month = date[1]
 # mb = self.DATA.isel(time = (self.xopen.time.dt.year == year))
 # mb = mb.isel(time.dt.month=(month-1))#do double check this but ygm
 # I'm also going to be starting from a restart file so I'll need to check some things to ensure that doesnt fuck up
 # But, I only need to add the restart data to the DATA file so it'll be fine.(potentially)
 # Make a function to get mb by calling cosipymb.get_monthly_mb,
 #probably something like: DATA.MASK = DATA.HGT.where(heights+ice_thk-heights !=0.1)
 #then selecting the date info from above, getting and starting from the restart dataset
 #(just do config.restart = true? ) 
 
##POSTPROCESSING: difference start from end extent, get input weather data from it and how much water there is
 # do that + summedQ from cosipy to get total water out, profit.
## PREPROCESSING: run cosipy for one day to get restart data.
# use ice thickness data to make glacier mask as realisticly it's not got ice on it
#put ice thickness data into cosipy- to start from.
#!! put thickness data into cosipy if glacier expands(it won't do here?)

#ALSO: check how different the calculated ice thicknesses are.
    def get_monthly_mb(heights,year):
        RESTART = IO.get_restart()
        RESULT = IO.get_result()
        out = RESULT.where(heights!=0)
        

def run_mbmod(cluster,DATA,IO,RESULT,RESTART,futures):
    
    
    IO = IOClass()
    DATA = self.DATA
    RESULT = IO.create_result_file()
    RESTART = IO.create_restart_file()
    Config()
    Constants()
    
    for y,x in product(range(DATA.sizes[Config.northing]),range(DATA.sizes[Config.easting])):
        mask = DATA.MASK.isel(lat=y, lon=x)
        if (mask == 1) and (Config.restart):
                        check_for_nan(data=DATA.isel(lat=y,lon=x))
                        futures.append(
                            client.submit(
                                cosipy_core,
                                DATA.isel(lat=y, lon=x),
                                y,
                                x,
                                GRID_RESTART=IO.create_grid_restart().isel(
                                    lat=y, lon=x
                                ),
                            )
                        )
        # Finally, do the calculations and print the progress
        progress(futures)
    IO.create_global_result_arrays()

    # Create numpy arrays which aggregates all local results
    IO.create_global_restart_arrays()
    
    for future in as_completed(futures):

            # Get the results from the workers
            indY, indX, local_restart, RAIN, SNOWFALL, LWin, LWout, H, LE, B, \
                QRR, MB, surfMB, Q, SNOWHEIGHT, TOTALHEIGHT, TS, ALBEDO, \
                NLAYERS, ME, intMB, EVAPORATION, SUBLIMATION, CONDENSATION, \
                DEPOSITION, REFREEZE, subM, Z0, surfM, new_snow_height, new_snow_timestamp, old_snow_timestamp, MOL, LAYER_HEIGHT, \
                LAYER_RHO, LAYER_T, LAYER_LWC, LAYER_CC, LAYER_POROSITY, \
                LAYER_ICE_FRACTION, LAYER_IRREDUCIBLE_WATER, LAYER_REFREEZE, \
                stake_names, stat, df_eval = future.result()

            IO.copy_local_to_global(
                indY, indX, RAIN, SNOWFALL, LWin, LWout, H, LE, B, QRR, MB, surfMB, Q,
                SNOWHEIGHT, TOTALHEIGHT, TS, ALBEDO, NLAYERS, ME, intMB, EVAPORATION,
                SUBLIMATION, CONDENSATION, DEPOSITION, REFREEZE, subM, Z0, surfM, MOL,
                LAYER_HEIGHT, LAYER_RHO, LAYER_T, LAYER_LWC, LAYER_CC, LAYER_POROSITY,
                LAYER_ICE_FRACTION, LAYER_IRREDUCIBLE_WATER, LAYER_REFREEZE)

            IO.copy_local_restart_to_global(indY,indX,local_restart)
            # Write results to file
            IO.write_results_to_file()

            # Write restart data to file
            MB_out=IO.write_restart_to_file()
            
    return MB_out
    
def create_data_directory(path: str) -> str:
    """Create a directory in the configured data folder.

    Returns:
        Path to the created directory.
    """
    dir_path = os.path.join(Config.data_path, path)
    os.makedirs(dir_path, exist_ok=True)

    return dir_path


def get_timestamp_label(timestamp: str) -> str:
    """Get a formatted label from a timestring.

    Args:
        An ISO 8601 timestamp.

    Returns:
        Formatted timestamp with hyphens and time removed.
    """
    return (timestamp[0:10]).replace("-", "")


def set_output_netcdf_path() -> str:
    """Set the file path for the output netCDF file.

    Returns:
        The path to the output netCDF file.
    """
    time_start = get_timestamp_label(timestamp=Config.time_start)
    time_end = get_timestamp_label(timestamp=Config.time_end)
    output_path = f"{Config.output_prefix}_{time_start}-{time_end}.nc"

    return output_path


def start_logging():
    """Start the python logging"""

    if os.path.exists('./cosipy.yaml'):
        with open('./cosipy.yaml', 'rt') as f:
            config = yaml.load(f.read(),Loader=yaml.SafeLoader)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)
    logger.info('COSIPY simulation started')


def transform_coordinates(coords):
    """Transform geodetic coordinates to cartesian."""
    # WGS 84 reference coordinate system parameters
    A = 6378.137  # major axis [km]
    E2 = 6.69437999014e-3  # eccentricity squared

    coords = np.asarray(coords).astype(float)

    # is coords a tuple? Convert it to an one-element array of tuples
    if coords.ndim == 1:
        coords = np.array([coords])

    # convert to radiants
    lat_rad = np.radians(coords[:,0])
    lon_rad = np.radians(coords[:,1])

    # convert to cartesian coordinates
    r_n = A / (np.sqrt(1 - E2 * (np.sin(lat_rad) ** 2)))
    x = r_n * np.cos(lat_rad) * np.cos(lon_rad)
    y = r_n * np.cos(lat_rad) * np.sin(lon_rad)
    z = r_n * (1 - E2) * np.sin(lat_rad)

    return np.column_stack((x, y, z))


def compute_scale_and_offset(min, max, n):
    # stretch/compress data to the available packed range
    scale_factor = (max - min) / (2 ** n - 1)
    # translate the range to be symmetric about zero
    add_offset = min + 2 ** (n - 1) * scale_factor
    return (scale_factor, add_offset)


@gen.coroutine
def close_everything(scheduler):
    yield scheduler.retire_workers(workers=scheduler.workers, close_workers=True)
    yield scheduler.close()


def print_notice(msg:str):
    print(f"{'-'*72}\n{msg}\n{'-'*72}\n")


def check_for_nan(data):
    if np.isnan(data.to_array()).any():
        raise SystemExit('ERROR! There are NaNs in the dataset.')


    
        

