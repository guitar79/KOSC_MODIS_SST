#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Wed Oct 21 14:38:31 2020
@author: guitar79
created by Kevin
#Open hdf file
NameError: name 'SD' is not defined
conda install -c conda-forge pyhdf

runfile('/mnt/14TB1/RS-data/KOSC/KOSC_MODIS_SST_Python/statistics_MODIS_hdf_all_MP-01.py', 'daily', wdir='/mnt/14TB1/RS-data/KOSC/KOSC_MODIS_SST_Python')

runfile('/mnt/4TB1/RS-data/KOSC_MODIS_SST_Python/statistics_MODIS_hdf_all_MP-01.py', 'daily', wdir='/mnt/4TB1/RS-data/KOSC_MODIS_SST_Python')

runfile('./statistics_MODIS_hdf_all_MP-01.py', 'daily', wdir='./KOSC_MODIS_SST_Python/')


'''

from glob import glob
from datetime import datetime
import numpy as np
import os
import sys
from sys import argv
import MODIS_hdf_utility

script, L3_perid = argv # Input L3_perid : 'weekly' 'monthly' 'daily'
if L3_perid == 'daily' or L3_perid == 'weekly' or L3_perid == 'monthly' :
    print(L3_perid, 'processing started')
else :
    print('input daily weekly monthly')
    sys.exit()

add_log = True
if add_log == True :
    log_file = 'MODIS_SST_python.log'
    err_log_file = 'MODIS_SST_python.log'
    
#for checking time
cht_start_time = datetime.now()

import multiprocessing as proc

myQueue = proc.Manager().Queue()

# I love the OOP way.(Custom class for multiprocessing)
class Multiprocessor():
    def __init__(self):
        self.processes = []
        self.queue = proc.Queue()

    @staticmethod
    def _wrapper(func, args, kwargs):
        ret = func(*args, **kwargs)
        myQueue.put(ret)

    def restart(self):
        self.processes = []
        self.queue = proc.Queue()

    def run(self, func, *args, **kwargs):
        args2 = [func, args, kwargs]
        p = proc.Process(target=self._wrapper, args=args2)
        self.processes.append(p)
        p.start()

    def wait(self):
        for p in self.processes:
            p.join()
        rets = []
        for p in self.processes:
            ret = myQueue.get_nowait()

            rets.append(ret)
        for p in self.processes:
            p.terminate()
        return rets

DATAFIELD_NAME = "sst4"
#Set lon, lat, resolution
Llon, Rlon = 115, 145
Slat, Nlat = 20, 50
resolution = 0.05
base_dir_name = '../MODIS_L2_SST/'
save_dir_name = "../{0}_L3/{0}_{1}_{2}_{3}_{4}_{5}_{6}/".format(DATAFIELD_NAME, str(Llon), str(Rlon),
                                                                str(Slat), str(Nlat), str(resolution), L3_perid)

if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print ('*'*80)
    print (save_dir_name, 'is created')
else :
    print ('*'*80)
    print (save_dir_name, 'is exist')

myMP = Multiprocessor()

years = range(2013, 2014)

proc_dates = []

#make processing period tuple
for year in years:
    dir_name = base_dir_name + str(year) + '/'

    from dateutil.relativedelta import relativedelta
    s_start_date = datetime(year, 1, 1) #convert startdate to date type
    s_end_date = datetime(year+1, 1, 1)

    k=0
    date1 = s_start_date
    date2 = s_start_date
    
    while date2 < s_end_date :
        k += 1
        if L3_perid == 'daily' :
            date2 = date1 + relativedelta(days=1)
        elif L3_perid == 'weekly' :
            date2 = date1 + relativedelta(days=8)
        elif L3_perid == 'monthly' :
            date2 = date1 + relativedelta(months=1)
        #date1_strf = date1.strftime('%Y%m%d')
        #date2_strf = date2.strftime('%Y%m%d')
        #date = (date1_strf, date2_strf, k)
        date = (date1, date2, k)
        proc_dates.append(date)
        date1 = date2

#### make dataframe from file list
fullnames = sorted(glob(os.path.join(base_dir_name, '*.hdf')))

fullnames_dt = []
for fullname in fullnames :
    fullnames_dt.append(MODIS_hdf_utility.fullname_to_datetime_for_KOSCSST(fullname))

# import pandas as pd 
import pandas as pd 

len(fullnames)
len(fullnames_dt)

# Calling DataFrame constructor on list 
df = pd.DataFrame({'fullname':fullnames,'fullname_dt':fullnames_dt})
df.index = df['fullname_dt']
df

num_cpu = 3

values = []

num_batches = len(proc_dates) // num_cpu + 1

for batch in range(num_batches):

    #myMP.restart()

    for proc_date in proc_dates[batch*num_cpu:(batch+1)*num_cpu]:

        df_proc = df[(df['fullname_dt'] >= proc_date[0]) & (df['fullname_dt'] < proc_date[1])]
        
        if os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_result.npy'\
                .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))\
            and os.path.exists('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
                .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) :
                
            print(('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are exist...'\
                .format(save_dir_name, DATAFIELD_NAME, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))))
        
        else : 
    
            if len(df_proc) != 0 :
                
                print("df_proc: {}".format(df_proc))
            
                processing_log = "#This file is created using Python : https://github.com/guitar79/KOSC_MODIS_SST\n"
                processing_log += "#L3_perid = {}, start date = {}, end date = {}\n"\
                    .format(L3_perid, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'))
        
                processing_log += "#Llon = {}, Rlon = {}, Slat = {}, Nlat = {}, resolution = {}\n"\
                    .format(str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))
                
                # make lat_array, lon_array, array_data
                print("{0}-{1} Start making grid arrays...\n".\
                      format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
                array_data = MODIS_hdf_utility.make_grid_array(Llon, Rlon, Slat, Nlat, resolution)
                
                print('Grid arrays are created...........\n')
            
                    #proc_start_date = proc_date[0].strftime('%Y%m%d')
                    #proc_end_date = proc_date[1].strftime('%Y%m%d')
                    #thread_number = proc_date[2]

                total_data_cnt = 0
                file_no = 0
                processing_log += '#processing file list\n'
                processing_log += '#file No, data_count, filename\n'

                for fullname in df_proc["fullname"] : 
                    array_alldata = array_data.copy()
                    try:
                        print('reading hdf file {0}\n'.format(fullname))
                        latitude, longitude, hdf_value, cntl_pt_cols, cntl_pt_rows \
                            = MODIS_hdf_utility.read_MODIS_hdf_to_ndarray(fullname, DATAFIELD_NAME)

                        print("latitude: {}".format(latitude))
                        print("longitude: {}".format(longitude))
                        print("hdf_value: {}".format(hdf_value))
                        print("np.shape(latitude): {}".format(np.shape(latitude)))
                        
                        print("np.shape(longitude): {}".format(np.shape(longitude)))
                        print("np.shape(hdf_value): {}".format(np.shape(hdf_value)))
                        print("len(cntl_pt_cols): {}".format(len(cntl_pt_cols)))
                        print("len(cntl_pt_rows): {}".format(len(cntl_pt_rows)))
                        
                    except Exception as err :
                        print("Something got wrecked : {}".format(err))    
                        
                    if np.shape(latitude) == np.shape(longitude) :
                        if np.shape(latitude)[0] != np.shape(hdf_value)[0] :
                            print("np.shape(latitude)[0] != np.shape(hdf_value)[0] is not same...")
                            row = 0
                            latitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(latitude[0])) :
                                for i in range(len(cntl_pt_rows)-1) :
                                    latitude_value = np.linspace(latitude[row,i], latitude[row,i+1], cntl_pt_rows[i])
                                    for j in range(i) :
                                        latitude_new[row, row+j] = latitude_value[j]                        
                            print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                                                          
                        elif np.shape(latitude)[1] != np.shape(hdf_value)[1] :
                            print("np.shape(latitude)[1] != np.shape(hdf_value)[1] is true...")
                            col = 0
                            latitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(latitude[1])) :
                                for i in range(len(cntl_pt_cols)-1) :
                                    latitude_value = np.linspace(latitude[row,i], \
                                                                 latitude[row,i+1], \
                                                                 cntl_pt_cols[i+1]-cntl_pt_cols[i]+1)
                                    #print("latitude_value {}: {}".format(i, latitude_value))
                                    #print("{0}, cntl_pt_cols[{1}]-cntl_pt_cols[{0}] : {2})"\
                                    #      .format(i, i+1, cntl_pt_cols[i+1]-cntl_pt_cols[i]))
                                    for j in range(len(latitude_value)-1) :
                                        latitude_new[row, cntl_pt_cols[i]-1+j] = latitude_value[j] 
                                    latitude_new[row, np.shape(latitude_new)[1]-1] = latitude[row, np.shape(latitude)[1]-1]
                            print("np.shape(latitude_new): {}".format(np.shape(latitude_new)))
                            
                        latitude = latitude_new.copy()
                        print("np.shape(latitude): {}".format(np.shape(latitude)))
                        
                        if np.shape(longitude)[0] != np.shape(hdf_value)[0] :
                            print("np.shape(longitude)[0] != np.shape(hdf_value)[0] is true...")
                            row = 0
                            longitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(longitude[0])) :
                                for i in range(len(cntl_pt_rows)-1) :
                                    longitude_value = np.linspace(longitude[row,i], longitude[row,i+1], cntl_pt_rows[i])
                                    for j in range(i) :
                                        longitude_new[row, row+j] = longitude_value[j]                        
                            print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                                                    
                        elif np.shape(longitude)[1] != np.shape(hdf_value)[1] :
                            print("np.shape(longitude)[1] != np.shape(hdf_value)[1] is true...")
                            col = 0
                            longitude_new = np.empty(shape=(np.shape(hdf_value)))
                            for row in range(len(longitude[1])) :
                                for i in range(len(cntl_pt_cols)-1) :
                                    longitude_value = np.linspace(longitude[row,i], \
                                                                 longitude[row,i+1], \
                                                                 cntl_pt_cols[i+1]-cntl_pt_cols[i]+1)
                                    #print("longitude_value {}: {}".format(i, latitude_value))
                                    #print("{0}, cntl_pt_cols[{1}]-cntl_pt_cols[{0}] : {2})"\
                                    #      .format(i, i+1, cntl_pt_cols[i+1]-cntl_pt_cols[i]))
                                    for j in range(len(longitude_value)-1) :
                                        longitude_new[row, cntl_pt_cols[i]-1+j] = longitude_value[j] 
                                    longitude_new[row, np.shape(longitude_new)[1]-1] = longitude[row, np.shape(longitude)[1]-1]
                            print("np.shape(longitude_new): {}".format(np.shape(longitude_new)))
                            
                        longitude = longitude_new.copy()
                        print("np.shape(longitude): {}".format(np.shape(longitude)))
                                            
                    if np.shape(latitude) == np.shape(hdf_value) \
                        and np.shape(longitude) == np.shape(hdf_value) :
                            
                        longitude[longitude > Llon] = np.nan
                        longitude[longitude < Rlon] = np.nan
                        latitude[latitude > Nlat] = np.nan
                        latitude[latitude < Slat] = np.nan
                            
                        #lon_cood = np.array((((longitude-Llon)/resolution*100)//100), dtype=np.uint16)
                        #lat_cood = np.array((((Nlat-latitude)/resolution*100)//100), dtype=np.uint16)
                        
                        lon_cood = np.array((((longitude-Llon)/resolution*100)//100))
                        lat_cood = np.array((((Nlat-latitude)/resolution*100)//100))
                        
                        print("np.shape(lon_cood): {}".format(np.shape(lon_cood)))
                        print("np.shape(lat_cood): {}".format(np.shape(lat_cood)))
                        print("lon_cood: {}".format(lon_cood))
                        print("lat_cood: {}".format(lat_cood))
                        
                        data_cnt = 0
                        NaN_cnt = 0
                        for i in range(np.shape(lon_cood)[0]) :
                            for j in range(np.shape(lon_cood)[1]) :
                                if not np.isnan(lon_cood[i][j]) \
                                    and not np.isnan(lat_cood[i][j]) \
                                    and not np.isnan(hdf_value[i][j]) :
                                    data_cnt += 1
                                    array_alldata[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                                    #print("array_alldata[{}][{}].append({})"\
                                    #      .format(int(lon_cood[i][j]), int(lat_cood[i][j]), hdf_value[i][j]))
                                    print("{} data added...".format(data_cnt))
                               
                        file_no += 1
                        total_data_cnt += data_cnt
                        processing_log += str(file_no) + ',' + str(data_cnt) +',' + str(fullname) + '\n'
                    else :
                        print("np.shape(latitude) == np.shape(hdf_value) and np.shape(longitude == np.shape(hdf_value) is not true...")
                                    
                processing_log += '#total data number =' + str(total_data_cnt) + '\n'
                
                print("array_alldata: {}".format(array_alldata))
                #print("prodessing_log: {}".format(processing_log))
                break
                
                print("{} \n {} - {}, 'Calculating mean value at each pixel is being started...\n"\
                      .format('='*80, proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
                
                array_mean = array_data.copy()
                array_count = array_data.copy()
                
                for i in range(np.shape(array_alldata)[0]):
                    for j in range(np.shape(array_alldata)[1]):
                        print("array_alldata[{}][{}]: {}".format(i, j, array_alldata[i][j]))
                        
                        if len(array_alldata[i][j]) > 0: 
                            array_mean[i][j] = array_mean[i][j].append(np.mean(array_alldata[i][j]))
                        else : 
                            array_mean[i][j] = array_mean[i][j].append(np.nan)
                        array_count[i][j] = array_count[i][j].append(len(array_alldata[i][j]))
                    
                    print("array_alldata[{}][{}] {:.01f} % : {}".format(i, j, \
                           i/np.shape(array_alldata)[0]*100, array_alldata[i][j]))
                        
                array_mean = np.array(array_mean)
                array_count = np.array(array_count)
                array_save = np.array([array_lon, array_lat, array_mean, array_count])
                                            
                np.save('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_result.npy'\
                    .format(save_dir_name, DATAFIELD_NAME, 
                    proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), 
                    str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), array_save)
                
                with open('{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8}_info.txt'\
                      .format(save_dir_name, DATAFIELD_NAME, \
                      proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                      str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
                    f.write(processing_log)
                print('#'*60)
                MODIS_hdf_utility.write_log(log_file, \
                    '{0}{1}_{2}_{3}_{4}_{5}_{6}_{7}_{8} files are is created.'\
                    .format(save_dir_name, DATAFIELD_NAME, \
                    proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d'), \
                    str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))

            else :
                print("There is no data in {0} - {1} ...\n"\
                      .format(proc_date[0].strftime('%Y%m%d'), proc_date[1].strftime('%Y%m%d')))
            
