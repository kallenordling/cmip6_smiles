#!/usr/bin/env python
# coding: utf-8

# In[1]:


import numpy as np
import gcsfs
import pandas as pd
import xarray as xr
import warnings
import matplotlib.pyplot as plt

import zarr
import dask.array as da  
from dask.diagnostics import ProgressBar
from tqdm.autonotebook import tqdm
import nc_time_axis
import time
import regionmask
import fsspec
import re


# In[3]:


df = pd.read_csv('https://storage.googleapis.com/cmip6/cmip6-zarr-consolidated-stores.csv')
df = df[ (df.table_id.isin(['day','Amon'])) & (df.variable_id.isin(['tasmax','tas','tasmin'])) & df.experiment_id.isin(['historical','ssp129','ssp119','ssp245','ssp370']) & df.activity_id.isin(['CMIP','ScenarioMIP'])]

sel = df.member_id.map(lambda x: bool(re.search('r*i1p1f1',x)))
df2=df[sel]
source_ids = ['CanESM5','MIROC6']


# In[4]:


def load_data(source_id, expt_id,var,table_id):
	print('load data')
	#get list of urls
	uri = df2[(df2.source_id == source_id) & (df2.experiment_id == expt_id) & (df2.variable_id == var) & (df2.table_id == table_id)].zstore
	#get list of member_id's
	member_id = df2[(df2.source_id == source_id) & (df2.experiment_id == expt_id) & (df2.variable_id == var) & (df2.table_id == table_id)].member_id#.values[0]
	#concentrate all members to single xarray dataframe
	dss = []
	for ur in uri.values:
		dss.append(xr.open_zarr(fsspec.get_mapper(ur), consolidated=True))
	if len(dss) > 0:
		ds= xr.concat(dss,dim='member_id').assign_coords(member_id=list(member_id))
		return ds
	else:
		return None

results={}
for exp in ['historical','ssp129','ssp119','ssp245','ssp370']:
	results[exp]={}
	for model in source_ids:
		results[exp][model]=load_data(model,exp,'tas','Amon')


# In[6]:


def calcMean(data):
	if data is None:
		return None
	weights = np.cos(np.deg2rad(data.lat))
	weights.name = "weights"
	data_weighted = data.weighted(weights)
	data_mean= data.mean(("lon", "lat"))
	return data_mean.groupby('time.year').mean()

gmst_dicts = {}
print('calc means')
for exp, dic in results.items():
	tmp = {}

	for model, data in dic.items():
		tmp[model] = calcMean(data)
	gmst_dicts[exp] = tmp


# In[7]:


print('calc anomns')
gmst_anoms={}
for exp, data in gmst_dicts.items():
	gmst_anoms[exp] = {}
	for model in ['CanESM5','MIROC6']:
		if data[model] is None:
			continue
		gmst_anoms[exp][model] = (data[model] - (gmst_dicts['historical'][model].sel(year=slice('1850','1900')).mean(dim=['year','member_id']))).load()#.groupby('year').mean()


# In[8]:


select_years={}
for i,exp in enumerate(['historical','ssp129','ssp119','ssp245','ssp370']):
	select_years[exp] = {}
	for model in ['CanESM5','MIROC6']:
		select_years[exp][model] = {}
		try:
			for id in gmst_anoms[exp][model].tas.member_id.values:
				select_years[exp][model][id] = {}
				tmp=gmst_anoms[exp][model].sel(member_id=id)
				for lev in [1,1.5,2,3,4]:
					tmp2=tmp.where((tmp.tas > (lev-0.25)) & (tmp.tas < (lev+0.25)),drop=True)
					select_years[exp][model][id][lev] = tmp2
		except:
        		select_years[exp][model] = None
        		continue


# In[9]:


def load_data(source_id, expt_id,var,table_id):
    #get list of urls
    uri = df2[(df2.source_id == source_id) & (df2.experiment_id == expt_id) & (df2.variable_id == var) & (df2.table_id == table_id)].zstore
    #get list of member_id's
    member_id = df2[(df2.source_id == source_id) & (df2.experiment_id == expt_id) & (df2.variable_id == var) & (df2.table_id == table_id)].member_id#.values[0]
    #concentrate all members to single xarray dataframe
    dss = []
    for ur in uri.values:
        dss.append(xr.open_zarr(fsspec.get_mapper(ur), consolidated=True))
    if len(dss) > 0:
        ds= xr.concat(dss,dim='member_id').assign_coords(member_id=list(member_id))
        return ds
    else:
        return None

results_day={}
for exp in ['historical','ssp129','ssp119','ssp245','ssp370']:
	results_day[exp]={}
	for model in source_ids:
		results_day[exp][model]=load_data(model,exp,'tasmax','day')


# In[10]:


def intersection(lst1, lst2): 
	return list(set(lst1) & set(lst2)) 

print('sambple data')
final_set={'CanESM5':{1:[],1.5:[],2:[],3:[],4:[]},'MIROC6':{1:[],1.5:[],2:[],3:[],4:[]}}
for exp in ['historical','ssp129','ssp119','ssp245','ssp370']:
	for model in source_ids:
		if select_years[exp][model] is None:
			continue
		for member,levels in select_years[exp][model].items():

			try:
				data=results_day[exp][model].sel(member_id=member).tasmax.groupby('time.year')
				data2=results_day[exp][model].sel(member_id=member)
				for lev,years in levels.items():
					if len(years.year) == 0:
						continue
					groups=intersection(list(data.groups.keys()),years.year.values)
					for year in groups:
						final_set[model][lev].append([data2.sel(time=str(year)).tasmax.max(axis=0),years.tas.sel(year=year).values,year])

			except:
				continue


# In[ ]:


land = regionmask.defined_regions.natural_earth.countries_110
for model in ['CanESM5','MIROC6']:
	for lev in [2]:#,1.5,2,3,4]:
		f = open("data_"+model+"_"+str(lev)+".csv", "w", buffering=1)
		line="region,year,lev,model,tasmax,global_mean"
		f.write(line+"\n")
		f.flush()
		for data in final_set[model][lev]:

			test_set=data[0]
			mask_3D = land.mask_3D(test_set)
			for region in mask_3D.abbrevs:
				#print(str(region.names.values),model,lev)
				try:
					r2 = mask_3D.isel(region=(mask_3D.abbrevs == region))
					temp=float(data[1])
					y = np.max(test_set.where(r2).load().to_masked_array().flatten())
					y = y[~np.isnan(y)]
					x = np.zeros(len(y))+temp
					line=str(region.names.values)+','+str(data[2])+','+str(lev)+','+model+','+str(float(y))+','+str(temp)
					f.write(line+"\n")
					f.flush()
					print('LINE')
					print(line)
				except:
					print('ERROR:',region.names)


		f.close()


# In[ ]:




