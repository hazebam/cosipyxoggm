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
from cosipyxoggm.SIAmodel import sia2d
from cosipyxoggm.config import Config, SlurmConfig
from cosipyxoggm.constants import Constants
from cosipyxoggm.cpkernel.cosipy_core import cosipy_core
from cosipyxoggm.cpkernel.io import IOClass
from oggm import cfg, utils
from oggm.utils import floatyear_to_date
from oggm.cfg import G, SEC_IN_YEAR, SEC_IN_DAY , SEC_IN_MONTH




class cosipymb():
    def __init__(self,ice_thk, topo = None ,yr = 1981 , DATA = None  , RESTART= None):
        #self.heights = heights
        self.DATA = DATA
        self.topo=topo
        self.yr = yr
        self.date = yr
        self.date = utils.floatyear_to_date(yr)
        self.year = self.date[0]
        self.month = self.date[1]
        self.IO = IOClass()
        self.RESTART = RESTART
        self.ice_thk = ice_thk
        self.DATAmonth = None
       
        
    ### topo = ndarray of topology,
        
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

    def get_monthly_mb(self,heights,year):
        #so I should be getting next month's data here and then pushing to run cosipy
        #TODO tomorrow: call IO to create restart files etc here
        #plan is to NOT use restart files and put in snowheight into DATA.
        #heights needs to be returned with dims
        date = utils.floatyear_to_date(year)
        year = date[0]
        month = date[1]
        DATA = self.DATA
        DATAsel = DATA.isel(time = (DATA.time.dt.year == year))
        DATAmonth = DATAsel.isel(time = (DATAsel.time.dt.month == month))
        self.DATAmonth = DATAmonth
        DATAmonth.compute()
        ice_thk = heights - self.topo 
        RESULT = cosipymb.main(self,DATA=DATAmonth)
        
        #DATA.MASK = RESULT.where(ice_thk!=0)
        self.RESTART = self.IO.write_restart_to_file()
        self.RESULT = self.IO.write_results_to_file()
        #Now make a new function that will take MB from here and mean it over time, then do the maths to make it into a way that oggm can use it.
        #Also please look at messing with config and IO to make this look much better and less janky
        #Also also please clean up this area as there's 4 functions that are the same and u only use one. 
        return RESULT
        

    def run_mbmod(self,cluster,DATA,IO,RESULT, RESTART, futures):
        IO=IO
        DATA = self.DATAmonth
        ice_thk = self.ice_thk
        Config()
        Constants()

        with Client(cluster) as client:
            print_notice(msg="\tStarting clients and submitting jobs ...")
            print(cluster)
            print(client)

            # Get dimensions of the whole domain
            # ny = DATA.sizes[Config.northing]
            # nx = DATA.sizes[Config.easting]

            # cp = cProfile.Profile()

            # Get some information about the cluster/nodes
            total_grid_points = DATA.sizes[Config.northing]*DATA.sizes[Config.easting]
            if Config.slurm_use:
                total_cores = SlurmConfig.cores * SlurmConfig.nodes
                points_per_core = total_grid_points // total_cores
                print(total_grid_points, total_cores, points_per_core)
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
                IO.write_restart_to_file()
                
    def main(self,DATA):
        Config()
        Constants()

        start_logging()

        #------------------------------------------
        # Create input and output dataset
        #------------------------------------------
        IO = IOClass(DATA)
        DATAa = DATA

        # Create global result and restart datasets
        RESULT = IO.create_result_file()
        RESTART = IO.create_restart_file()
        DATA = IO.create_data_file(DATA)

        #----------------------------------------------
        # Calculation - Multithreading using all cores
        #----------------------------------------------

        # Auxiliary variables for futures
        futures = []

        # Measure time
        start_time = datetime.now()

        #-----------------------------------------------
        # Create a client for distributed calculations
        #-----------------------------------------------
        if Config.slurm_use:
            SlurmConfig()
            with SLURMCluster(
                job_name=SlurmConfig.name,
                cores=SlurmConfig.cores,
                processes=SlurmConfig.cores,
                memory=SlurmConfig.memory,
                account=SlurmConfig.account,
                job_extra_directives=SlurmConfig.slurm_parameters,
                local_directory=SlurmConfig.local_directory,
            ) as cluster:
                cluster.scale(SlurmConfig.nodes * SlurmConfig.cores)
                print(cluster.job_script())
                print("You are using SLURM!\n")
                print(cluster)
                cosipymb.run_cosipy(cluster, IO, DATAa, RESULT, RESTART, futures)

        else:
            with LocalCluster(scheduler_port=Config.local_port, n_workers=Config.workers, local_directory='logs/dask-worker-space', threads_per_worker=1, silence_logs=True) as cluster:
                print(cluster)
                cosipymb.run_cosipy(self,cluster, IO, DATAa, RESULT, RESTART, futures)

        print("\n")
        print_notice(msg="Write results ...")
        start_writing = datetime.now()

        #-----------------------------------------------
        # Write results and restart files
        #-----------------------------------------------
        timestamp = pd.to_datetime(str(IO.get_restart().time.values)).strftime('%Y-%m-%dT%H-%M')
        
        encoding = dict()
        for var in IO.get_result().data_vars:
            # dataMin = IO.get_result()[var].min(skipna=True).values
            # dataMax = IO.get_result()[var].max(skipna=True).values
            # dtype = 'int16'
            # FillValue = -9999
            # scale_factor, add_offset = compute_scale_and_offset(dataMin, dataMax, 16)
            #encoding[var] = dict(zlib=True, complevel=compression_level, dtype=dtype, scale_factor=scale_factor, add_offset=add_offset, _FillValue=FillValue)
            encoding[var] = dict(zlib=True, complevel=Config.compression_level)
        output_netcdf = set_output_netcdf_path()
        output_path = create_data_directory(path='output')
        IO.get_result().to_netcdf(os.path.join(output_path,output_netcdf), encoding=encoding, mode='w')
        RESULTS = IO.get_result()
        encoding = dict()
        for var in IO.get_restart().data_vars:
            # dataMin = IO.get_restart()[var].min(skipna=True).values
            # dataMax = IO.get_restart()[var].max(skipna=True).values
            # dtype = 'int16'
            # FillValue = -9999
            # scale_factor, add_offset = compute_scale_and_offset(dataMin, dataMax, 16)
            #encoding[var] = dict(zlib=True, complevel=compression_level, dtype=dtype, scale_factor=scale_factor, add_offset=add_offset, _FillValue=FillValue)
            encoding[var] = dict(zlib=True, complevel=Config.compression_level)

        restart_path = create_data_directory(path='restart')
        IO.get_restart().to_netcdf(os.path.join(restart_path,f'restart_{timestamp}.nc'), encoding=encoding)

        #-----------------------------------------------
        # Stop time measurement
        #-----------------------------------------------
        duration_run = datetime.now() - start_time
        duration_run_writing = datetime.now() - start_writing

        #-----------------------------------------------
        # Print out some information
        #-----------------------------------------------
        get_time_required(
            action="write restart and output files", times=duration_run_writing
        )
        run_time = duration_run.total_seconds()
        print(f"\tTotal run duration: {run_time // 60.0:4g} minutes {run_time % 60.0:2g} seconds\n")
        print_notice(msg="\tSIMULATION WAS SUCCESSFUL")
        return RESULTS


    def run_cosipy(self,cluster, IO, DATA, RESULT, RESTART, futures):
        IO=IO
        DATA = self.DATAmonth
        ice_thk = self.ice_thk
        Config()
        Constants()

        with Client(cluster) as client:
            print_notice(msg="\tStarting clients and submitting jobs ...")
            print(cluster)
            print(client)

            # Get dimensions of the whole domain
            # ny = DATA.sizes[Config.northing]
            # nx = DATA.sizes[Config.easting]

            # cp = cProfile.Profile()

            # Get some information about the cluster/nodes
            total_grid_points = DATA.sizes[Config.northing]*DATA.sizes[Config.easting]
            if Config.slurm_use:
                total_cores = SlurmConfig.cores * SlurmConfig.nodes
                points_per_core = total_grid_points // total_cores
                print(total_grid_points, total_cores, points_per_core)

            # Check if evaluation is selected:
            if Config.stake_evaluation:
                # Read stake data (data must be given as cumulative changes)
                df_stakes_loc = pd.read_csv(Config.stakes_loc_file, delimiter='\t', na_values='-9999')
                df_stakes_data = pd.read_csv(Config.stakes_data_file, delimiter='\t', index_col='TIMESTAMP', na_values='-9999')
                df_stakes_data.index = pd.to_datetime(df_stakes_data.index)

                # Uncomment, if stake data is given as changes between measurements
                # df_stakes_data = df_stakes_data.cumsum(axis=0)

                # Init dataframes to store evaluation statistics
                df_stat = pd.DataFrame()
                df_val = df_stakes_data.copy()

                # reshape and stack coordinates
                if Config.WRF:
                    coords = np.column_stack((DATA.lat.values.ravel(), DATA.lon.values.ravel()))
                else:
                    # in case lat/lon are 1D coordinates
                    lons, lats = np.meshgrid(DATA.lon,DATA.lat)
                    coords = np.column_stack((lats.ravel(),lons.ravel()))

                # construct KD-tree, in order to get closes grid cell
                ground_pixel_tree = scipy.spatial.cKDTree(transform_coordinates(coords))

                # Check for stake data
                stakes_list = []
                for index, row in df_stakes_loc.iterrows():
                    index = ground_pixel_tree.query(transform_coordinates((row['lat'], row['lon'])))
                    if Config.WRF:
                        index = np.unravel_index(index[1], DATA.lat.shape)
                    else:
                        index = np.unravel_index(index[1], lats.shape)
                    stakes_list.append((index[0][0], index[1][0], row['id']))

            else:
                stakes_loc = None
                df_stakes_data = None

            # Distribute data and model to workers
            start_res = datetime.now()
            for y,x in product(range(DATA.sizes[Config.northing]),range(DATA.sizes[Config.easting])):
                if Config.stake_evaluation:
                    stake_names = []
                    # Check if the grid cell contain stakes and store the stake names in a list
                    for idx, (stake_loc_y, stake_loc_x, stake_name) in enumerate(stakes_list):
                        if (y == stake_loc_y) and (x == stake_loc_x):
                            stake_names.append(stake_name)
                else:
                    stake_names = None

                if Config.WRF:
                    mask = DATA.MASK.sel(south_north=y, west_east=x)
                    # Provide restart grid if necessary
                    if (mask == 1) and (not Config.restart):
                        check_for_nan(data=DATA.sel(south_north=y, west_east=x))
                        futures.append(client.submit(cosipy_core, DATA.sel(south_north=y, west_east=x), y, x, stake_names=stake_names, stake_data=df_stakes_data))
                    elif (mask == 1) and (Config.restart):
                        check_for_nan(data=DATA.sel(south_north=y, west_east=x))
                        futures.append(
                            client.submit(
                                cosipy_core,
                                DATA.sel(south_north=y, west_east=x),
                                y,
                                x,
                                GRID_RESTART=IO.create_grid_restart().sel(
                                    south_north=y,
                                    west_east=x,
                                ),
                                stake_names=stake_names,
                                stake_data=df_stakes_data
                            )
                        )
                else:
                    mask = DATA.MASK.isel(lat=y, lon=x)
                    # Provide restart grid if necessary
                    if (mask == 1) and (not Config.restart):
                        check_for_nan(data=DATA.isel(lat=y,lon=x))
                        futures.append(client.submit(cosipy_core, DATA.isel(lat=y, lon=x), y, x, stake_names=stake_names, stake_data=df_stakes_data,ice_thk=ice_thk.isel(lat=y, lon=x)))
                    elif (mask == 1) and (Config.restart):
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
                                stake_names=stake_names,
                                stake_data=df_stakes_data
                            )
                        )
            # Finally, do the calculations and print the progress
            progress(futures)

            #---------------------------------------
            # Guarantee that restart file is closed
            #---------------------------------------
            if Config.restart:
                IO.get_grid_restart().close()

            # Create numpy arrays which aggregates all local results
            IO.create_global_result_arrays()

            # Create numpy arrays which aggregates all local results
            IO.create_global_restart_arrays()

            #---------------------------------------
            # Assign local results to global
            #---------------------------------------
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
                IO.write_restart_to_file()

                if Config.stake_evaluation:
                    # Store evaluation of stake measurements to dataframe
                    stat = stat.rename('rmse')
                    df_stat = pd.concat([df_stat, stat])

                    for i in stake_names:
                        if Config.obs_type == 'mb':
                            df_val[i] = df_eval.mb
                        if Config.obs_type == 'snowheight':
                            df_val[i] = df_eval.snowheight

            # Measure time
            end_res = datetime.now()-start_res
            get_time_required(action="do calculations", times=end_res)

            if Config.stake_evaluation:
                # Save the statistics and the mass balance simulations at the stakes to files
                output_path = create_data_directory(path='output')
                df_stat.to_csv(os.path.join(output_path,'stake_statistics.csv'),sep='\t', float_format='%.2f')
                df_val.to_csv(os.path.join(output_path,'stake_simulations.csv'),sep='\t', float_format='%.2f')


                
    
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

    # is coords a tuple? Convert it to an one-element ndarray of tuples
    if coords.ndim == 1:
        coords = np.ndarray([coords])

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

def get_time_required(action:str, times):
    run_time = get_time_elapsed(times)
    print(f"\tTime required to {action}: {run_time}")


def get_time_elapsed(times) -> str:
    run_time = times.total_seconds()
    time_elapsed = f"{run_time//60.0:4g} minutes {run_time % 60.0:2g} seconds\n"
    return time_elapsed
    
        

