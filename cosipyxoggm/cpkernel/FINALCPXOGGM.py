#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 24 16:38:13 2025

@author: haze
"""


class mb_mod():
    
    def __init__(self,heights = None ,year = None , file = None):
        self.heights = heights
        date = utils.floatyear_to_date(year)
        self.year = date[0]
        self.month = date[1]
        self.file = '/Users/haze/Documents/JUPYTER/Modelling-Peru/mb-for-oggm.nc'
        self.xopen = xr.open_dataset(self.file)
        self.IO = IOClass()
        
    
    def get_monthly_mb(self,year):
        
        mb = IO.get_result().MB
        mb = mb.isel(time = (mb.time.dt.year == year))
        mb = mb.groupby('time.month').mean()
        mb = mb.isel(month=(month-1))#zero index
        mb = (mb/3600)*900 #cosipy output is hourly and oggm expects per second, using 900 as ice density for now
        return mb.data
    
    def update_cosipy(self):
        DATA = IO.get_result()
        MASK = DATA.MASK.where(MASK==1)
        DATA.MASK = DATA.where(heights-model.bed_topo!=0, 0)
        DATA = DATA.sel(time=slice(time_start, time_end))
        main(DATA)
        
        
        
        