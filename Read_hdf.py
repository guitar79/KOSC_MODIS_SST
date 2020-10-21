# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.

created by Kevin 
# 2018.07.22 
ubuntu 16.04, Anaconda 3
conda install basemap
conda install -c conda-forge pyhdf
"""

import numpy as np
from pyhdf.SD import SD, SDC

base_dr = '../L2_SST_MODIS/hdf/'
filename = 'MYDOCT.2018.1228.0443.aqua-1.hdf'

hdf = SD("{}{}".format(base_dr, filename), SDC.READ)

print (hdf.datasets())

'''
{
'year': (('Number of Scan Lines',), (4000,), 24, 0), 
'day': (('Number of Scan Lines',), (4000,), 24, 1), 
'msec': (('Number of Scan Lines',), (4000,), 24, 2), 
'tilt': (('Number of Scan Lines',), (4000,), 5, 3), 
'slon': (('Number of Scan Lines',), (4000,), 5, 4), 
'clon': (('Number of Scan Lines',), (4000,), 5, 5), 
'elon': (('Number of Scan Lines',), (4000,), 5, 6), 
'slat': (('Number of Scan Lines',), (4000,), 5, 7), 
'clat': (('Number of Scan Lines',), (4000,), 5, 8), 
'elat': (('Number of Scan Lines',), (4000,), 5, 9), 
'csol_z': (('Number of Scan Lines',), (4000,), 5, 10), 
'longitude': (('Number of Scan Lines', 'Number of Pixel Control Points'), (4000, 170), 5, 11), 
'latitude': (('Number of Scan Lines', 'Number of Pixel Control Points'), (4000, 170), 5, 12), 
'sst': (('Number of Scan Lines', 'Pixels per Scan Line'), (4000, 1354), 22, 13), 
'sst4': (('Number of Scan Lines', 'Pixels per Scan Line'), (4000, 1354), 22, 14), 
'l2_flags': (('Number of Scan Lines', 'Pixels per Scan Line'), (4000, 1354), 24, 15), 
'wavelength': (('total band number',), (17,), 24, 16), 
'cntl_pt_cols': (('Number of Pixel Control Points',), (170,), 24, 17), 
'cntl_pt_rows': (('Number of Scan Lines',), (4000,), 24, 18), 
'vcal_gain': (('band number',), (9,), 5, 19), 
'vcal_offset': (('band number',), (9,), 5, 20), 
'F0': (('band number',), (9,), 5, 21), 'k_oz': (('band number',), (9,), 5, 22), 
'Tau_r': (('band number',), (9,), 5, 23)
}
'''

DATAFIELD_NAME = 'sst'

sst_raw = hdf.select(DATAFIELD_NAME)
sst_attri = sst_raw.attributes()
sst_slope = sst_attri['slope']
sst_intercept = sst_attri['intercept']
sst = sst_raw.get()
sst = sst * sst_slope + sst_intercept
sst[sst < 0] = np.nan

#####
# Read geolocation dataset.
lat = hdf.select('latitude')
print(lat.attributes())
latitude = lat[:,:]
latitude1 = lat.get()
print(latitude == latitude1)

lon = hdf.select('longitude')
longitude = lon[:,:]
print(lon.attributes())

import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

m = Basemap(projection='cyl', resolution='l', llcrnrlat=10, urcrnrlat = 60, llcrnrlon=100, urcrnrlon = 160)
m.drawcoastlines(linewidth=0.5)
m.drawparallels(np.arange(-90, 90., 10.), labels=[1, 0, 0, 0])
m.drawmeridians(np.arange(-180., 180., 15.), labels=[0, 0, 0, 1])
x, y = m(longitude, latitude)
m.pcolormesh(x, y, sst, vmin=0,vmax=20)
plt.title('MODIS AOD')

#plt.colorbar()
plt.colorbar(cmap='rainbow', extend='max')

#plt.savefig('current{}.pdf'.format(time), bbox_inches='tight', dpi = 300)
#plt.savefig(f_name+'.png', bbox_inches='tight', dpi = 300)