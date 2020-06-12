#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 12 15:07:36 2020
EU ERA4CS DustClim
Script to access reanalysis results from BSC's THREDDS repository
@author: votsisa - Athanasios Votsis / FMI
"""
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import xarray as xr
import requests
import numpy as np
from dask.diagnostics import ProgressBar
import os


"""
The path that you should include in your Python script is the folllowing:

http://earth.bsc.es/thredds_dustclim/dodsC/monarch-dustclim/3hourly/${variable}-${statistics}_an/${variable}_${YYYYDDMM03}_${statistics}_an.nc
"""

# authentication
session = requests.Session()
session.auth = ('username', 'password') # fill in the real credentials here

# create url paths for extinction 550 variable and user-selected year(s), month(s), day(s)
urlhead = 'http://earth.bsc.es/thredds_dustclim/dodsC/monarch-dustclim/3hourly/ec550du-av_an/ec550du_'
urltail = '03_av_an.nc'
urls = [f'{urlhead}{year}{str(month).zfill(2)}{str(day).zfill(2)}{urltail}' for year in range(2011,2012) for month in range(1,2) for day in range(1,4)]
print(len(urls), urls)

# access multiple files with authenticated session
stores = []
for url in urls:
    store = xr.backends.PydapDataStore.open(url, session=session)
    stores.append(store)

# APPROACH 1: save one file at a time, with subsetting
os.chdir('/Users/votsisa/Datasets/DustClim/_FINAL-REANALYSIS/testing/files')
for url in urls:
    store = xr.backends.PydapDataStore.open(url, session=session)
    ds = xr.open_dataset(store)
    ds = ds['ec550du'][:,39,:,:]
    print(url[82:])
    ds.to_netcdf(url[82:])


# APPROACH 2: multifile straight to xarray and then subset
ds_whole = xr.open_mfdataset(stores, parallel=True, combine='by_coords')
ds_whole
ds = ds_whole['ec550du'][:,39,:,:]
ds

# save the filtered multifile dataset into one dataset and load
with ProgressBar(): ds.to_netcdf('/Users/votsisa/Datasets/DustClim/_FINAL-REANALYSIS/testing/manipulated-example-data.nc')
# and reuse it. NB! you have to load as dataarray (not dataset) in order to take advantage of the easy indexing
ds_mf = xr.open_dataarray('/Users/votsisa/Datasets/DustClim/_FINAL-REANALYSIS/testing/manipulated-example-data.nc', chunks={'time': 8, 'rlon':701, 'rlat':1021})
ds_mf
ds_mf[0,:,:]
ds_mf[29,:,:].plot.imshow(robust=True)
