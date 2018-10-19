''' created by Kevin 
  # 2018.07.22 
 ubuntu 16.04, Anaconda 3
 conda install basemap
 conda install -c conda-forge pyhdf
'''

#import os
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import numpy as np
#import pyhdf
from datetime import datetime
import time

#julianday = '%d%03d' % (datetime.now().timetuple().tm_year, datetime.now().timetuple().tm_yday)

# Open file.
#data = np.genfromtxt(FILE_NAME, delimiter="\t", skip_header=1, filling_values=-99) 
#FILE_NAME = '/media/guitar79/6T1/KOSC/L2_SST_NOAA/2011/2011.0901.0041.noaa-16.sst.asc'
dir_name = 'L3_SST_NOAA/2011/'
f_name = '2011.0901.0415.noaa-19.sst.asc'

with open(dir_name+f_name,'r') as tsv:
    sst_list = [line.strip().split('\t') for line in tsv]
#nsst = np.asarray(sst)

sst = []
lat = []
lon = []
for i in sst_list:
    row = [s.replace('***', '-99') for s in i]
    floatrow = [float(j) for j in row]
    lat.append(floatrow[1])
    lon.append(floatrow[2])
    sst.append(floatrow[3])

lat = np.array(lat)
lon = np.array(lon)
sst = np.array(sst)
lat = np.reshape(lat, (1, lat.shape[0]))
lon = np.reshape(lon, (1, lon.shape[0]))
sst = np.reshape(sst, (1, sst.shape[0]))
#sst[sst == -99] = np.nan

#lat = lat[:,:]
#lon = lon[:,:]
#sst = sst[:,:]

m = Basemap(projection='cyl', resolution='l', llcrnrlat=20, urcrnrlat = 50, llcrnrlon=115, urcrnrlon = 140)
m.drawcoastlines(linewidth=0.5)
m.drawparallels(np.arange(-90, 90., 10.), labels=[1, 0, 0, 0])
m.drawmeridians(np.arange(-180., 180., 10.), labels=[0, 0, 0, 1])
x, y = m(lon, lat)
m.pcolormesh(x, y, sst)
#m.pcolormesh(lon, lat, sst, vmin=0, vmax=30)
plt.title('NOAA SST')
plt.colorbar(cmap='bwr', extend='max')

#plt.savefig('current{}.pdf'.format(time), bbox_inches='tight', dpi = 300)
plt.savefig('current{}.png'.format(time), bbox_inches='tight', dpi = 300)
