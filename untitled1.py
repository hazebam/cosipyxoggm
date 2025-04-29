#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 24 15:16:27 2025

@author: haze
"""

class cosipymb():
    
    def __init__(self,heights = None ,year = None , DATA = None, date = None,):
        self.heights = heights
        self.year = year
        self.file = '/Users/haze/Documents/JUPYTER/Modelling-Peru/mb-for-oggm.nc'
        self.xopen = xr.open_dataset(self.file)
        self.year = date
        self.date = utils.floatyear_to_date(year)
        self.year = date[0]
        self.month = date[1]
        DATAsel = self.DATA.isel(time = (self.xopen.time.dt.year == year))
        self.DATA = mb.isel(month=(month-1))
        
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
    

def get_monthly_mb(self):
        Config()
        Constants()
        DATA = DATA_IN()
        futures = []
        with LocalCluster(scheduler_port=Config.local_port, n_workers=Config.workers, local_directory='logs/dask-worker-space', threads_per_worker=1, silence_logs=True) as cluster:
            print(cluster)
        run_cosipy(cluster, IO, DATA, RESULT, RESTART, futures)