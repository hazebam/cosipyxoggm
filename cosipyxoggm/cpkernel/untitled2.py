#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 24 16:26:47 2025

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


def get_monthly_mb(self, heights, year=None):
        """Get the monthly mass balance at given altitudes.
        
        Parameters
        ----------
        heights : ndarray
            The altitudes at which MB needs to be computed
        year : float
            The year for which MB needs to be computed
        fl_id : int, optional
            Flowline ID (unused in this implementation) 
        fls : list, optional
            Flowlines (unused in this implementation)
        
        Returns
        -------
        ndarray
            Monthly mass balance in m ice per second
        """
        # Prepare COSIPY grid for each height
        mb_at_h = []
        
        for h in heights:
            # Initialize COSIPY grid for this point
            grid = Grid(self.grid_data.sel(height=h))
            
            # Run COSIPY for one month
            mb = cosipy_core(grid, year=year)
            
            # Convert COSIPY mass balance (m w.e.) to m ice per second
            mb_ice_sec = mb['MB'] / (Constants.ice_density * 30 * 24 * 3600)
            
            mb_at_h.append(mb_ice_sec)
            
        return np.array(mb_at_h)