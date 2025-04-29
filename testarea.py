#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 27 13:09:29 2025

@author: haze
"""
#from oggm.core import sia2d
from cosipyxoggm.SIAmodel import sia2d
import xarray as xr
from dask.distributed import Client
import dask
from oggm import cfg, utils
from oggm.cfg import G, SEC_IN_YEAR, SEC_IN_DAY
cfg.initialize(logging_level='WARNING')
#cfg.PARAMS['use_multiprocessing'] = True
from cosipyxoggm import oggmxcpy as cp

###TEST AREA


# %%
gd = xr.open_dataset('/Users/haze/cosipyxoggm/gridded_data.nc',chunks = 'auto')
  
da = xr.open_dataset('/Users/haze/cosipyxoggm/data/input/weather/dataout',chunks = 'auto')

#static = xr.open_dataset('/Users/haze/cosipyxoggm/data/static/staticicethk')

mb_mod = cp.cosipymb(ice_thk =da.ICETHK.fillna(0) , topo = da.HGT ,yr = 1980 , DATA = da.fillna(0)  , RESTART= None)

model = sia2d.Upstream2D(bed_topo = da.HGT.values, 
                         init_ice_thick=da.ICETHK.fillna(0).values,
                         dx=25, mb_model=mb_mod, y0=1980,mb_filter=da.MASK.values==1,
                         mb_elev_feedback = 'always',glen_a=1,)    

#y= salem.open_xr_dataset('/Users/haze/cosipyxoggm/data/static/LHS-static-oggmbig.nc')

#with salem.open_xr_dataset('/Users/haze/cosipyxoggm/gridded_data.nc') as gd:
#    gd = gd.load() 
##28/04/25 ISSUE IS INPUT DATASET!!! (also, look @ using mlx/numpy for aws2cosipy)
#TODO: change IO init_data_dataset to work with the month calculation shit that i've whacked in oggmxcpy. This will make things a bit cleaner and just more like cosipy works
# %%