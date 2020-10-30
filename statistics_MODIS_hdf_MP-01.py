#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Wed Oct 21 14:38:31 2020
@author: guitar79
created by Kevin

'''

from glob import glob
from datetime import datetime
import numpy as np
import os
import sys
from sys import argv
import MODIS_hdf_utility

add_log = True
if add_log == True :
    log_file = 'MODIS_SST_python.log'
    err_log_file = 'MODIS_SST_python.log'
    
base_dir = '../sst4_L3/'

base_dir_names = []

fullnames = MODIS_hdf_utility.getFullnameListOfallFiles(base_dir)
print ("fullnames: {}".format(fullnames))
#fullname = fullnames[1]

for fullname in fullnames[:] :
    if fullname[-4:] == ".npy" :
        fullname_el = fullname.split("/")  
        filename_el = fullname_el[-1].split("_")
       
        save_dir_name = "{0}/{1}/{2}/statistics/".format(fullname_el[0], fullname_el[1], fullname_el[2])
    
        if not os.path.exists(save_dir_name):
            os.makedirs(save_dir_name)
            print ('*'*80)
            print (save_dir_name, 'is created')
        else :
            print ('*'*80)
            print (save_dir_name, 'is exist')
        
        array_alldata = np.load(fullname, allow_pickle=True)
        print("array_alldata:{}".format(array_alldata))
        
        start_date, end_date, Llon, Rlon, Slat, Nlat, resolution = \
            MODIS_hdf_utility.npy_filename_to_fileinfo(fullname)
        Llon, Rlon, Slat, Nlat, resolution = int(Llon), int(Rlon), int(Slat), int(Nlat), float(resolution)
        array_data = MODIS_hdf_utility.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
        
        array_mean = array_data.copy()
        array_count = array_data.copy()
        
        print(start_date, '-', end_date, 'Calculating mean value at each pixel is being started\n', '='*80)
        
        for i in range(np.shape(array_mean)[0]):
            for j in range(np.shape(array_mean)[1]):
                if len(array_alldata[i][j]) == 1: 
                    array_mean[i][j] = array_alldata[i][j][0][1].astype(np.float64)
                    print("array_alldata[i][j][0][1] ({},{}): {}".format(i, j, array_alldata[i][j][0][1]))
                elif len(array_alldata[i][j]) > 1: 
                    alldata = np.array(array_alldata[i][j])
                    array_mean[i][j] = np.mean(alldata[:,1].astype(np.float64))
                    print("np.mean(alldata[:,1]) ({},{}): {}".format(i, j, np.mean(alldata[:,1].astype(np.float64))))
                else : 
                    array_mean[i][j] = np.nan
                array_count[i][j] = len(array_alldata[i][j])
                #print("array_count[i][j] = len(array_alldata[i][j]) ({},{}): {}".format(i, j, len(array_alldata[i][j])))
        
        array_mean = np.array(array_mean)
        array_count = np.array(array_count)
        array_result = np.array([array_mean, array_count])
                
        print("array_mean: {}".format(array_mean))
        print("array_count: {}".format(array_count))
                                    
        np.save('{0}{1}_result.npy'\
            .format(save_dir_name, fullname_el[-1][:-4]), array_result)
        
        print('#'*60)
        MODIS_hdf_utility.write_log(log_file, \
            '{0}{1}_result.npy is created...'\
            .format(save_dir_name, fullname_el[-1][:-4]))

    