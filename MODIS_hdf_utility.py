'''
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
Created on Sat Nov  3 20:34:47 2018
@author: guitar79
created by Kevin
#Open hdf file
NameError: name 'SD' is not defined
conda install -c conda-forge pyhdf
'''

from glob import glob
import numpy as np
from datetime import datetime
import calendar
import os
from pyhdf.SD import SD, SDC

def write_log(log_file, log_str):
    with open(log_file, 'a') as log_f:
        log_f.write(log_str+'\n')
    return print (log_str)

#for checking time
cht_start_time = datetime.now()

#JulianDate_to_date(2018, 131) -- '20180511'
def JulianDate_to_date(y, jd):
    month = 1
    while jd - calendar.monthrange(y,month)[1] > 0 and month <= 12:
        jd = jd - calendar.monthrange(y,month)[1]
        month += 1
    #return datetime(y, month, jd).strftime('%Y%m%d')
    return datetime(y, month, jd)

#date_to_JulianDate('20180201', '%Y%m%d') -- 2018032
def date_to_JulianDate(dt, fmt):
    dt = datetime.strptime(dt, fmt)
    tt = dt.timetuple()
    return int('%d%03d' % (tt.tm_year, tt.tm_yday))

#for modis hdf file, filename = 'DAAC_MOD04_3K/H28V05/MOD04_3K.A2014003.0105.006.2015072123557.hdf'
def filename_to_datetime(filename):
    fileinfo = filename.split('.')
    #print('fileinfo', fileinfo)
    return JulianDate_to_date(int(fileinfo[-5][1:5]), int(fileinfo[-5][5:8]))

#for modis hdf file, filename = 'DAAC_MOD04_3K/daily/AOD_3K_20150101_20150102_90_150_10_60_0.1.npy'
def npy_filename_to_fileinfo(fullname):
    fileinfo = fullname.split('_')
    start_date = fileinfo[-8]
    end_date = fileinfo[-7]
    Llon = fileinfo[-6]
    Rlon = fileinfo[-5]
    Slat = fileinfo[-4]
    Nlat = fileinfo[-3]
    resolution = fileinfo[-2]
    return start_date, end_date, int(Llon), int(Rlon), int(Slat), int(Nlat), float(resolution)

def calculate_mean_using_result_array(result_array):
    mean_array = result_array.copy()
    cnt_array = result_array.copy()
    for i in range(np.shape(result_array)[0]):
        for j in range(np.shape(result_array)[1]):
            
            if len(result_array[i][j])>0: mean_array[i][j] = np.mean(result_array[i][j])
            else : mean_array[i][j] = np.nan
            cnt_array[i][j] = len(result_array[i][j])
    
    mean_array = np.array(mean_array)
    cnt_array = np.array(cnt_array)
    return mean_array, cnt_array

def make_grid_array(Llon, Rlon, Slat, Nlat, resolution) :
    ni = np.int((Rlon-Llon)/resolution+1.00)
    nj = np.int((Nlat-Slat)/resolution+1.00)
    lon_array = []
    lat_array = []
    data_array = []
    for i in range(ni):
        lon_line = []
        lat_line = []
        data_line = []
        for j in range(nj):
            lon_line.append(Llon+resolution*i)
            lat_line.append(Nlat-resolution*j)
            data_line.append([])
        lon_array.append(lon_line)
        lat_array.append(lat_line)
        data_array.append(data_line)
    lat_array = np.array(lat_array)
    lon_array = np.array(lon_array)
    return lat_array, lon_array, data_array


def read_MODIS_SST_hdf_and_array_by_date(save_dir_name, dir_name, proc_date, 
                             resolution, Llon, Rlon, Slat, Nlat):
    add_log = True
    if add_log == True :
        log_file = 'read_MODIS_AOD_hdf_and_array_by_date.log'
        err_log_file = 'read_MODIS_AOD_hdf_and_array_by_date_err.log'
    
    proc_start_date = proc_date[0]
    proc_end_date = proc_date[1]
    thread_number = proc_date[2]
    processing_log = '#This file is created using python \n' \
                '#https://github.com/guitar79/MODIS_AOD \n' \
                + '#start date = ' + str(proc_date[0]) +'\n'\
                + '#end date = ' + str(proc_date[1]) +'\n'
    #variables for downloading
    start_date = datetime(int(proc_start_date[:4]), 
                          int(proc_start_date[4:6]), 
                          int(proc_start_date[6:8])) #convert startdate to date type
    end_date = datetime(int(proc_end_date[:4]), 
                        int(proc_end_date[4:6]), 
                        int(proc_end_date[6:8])) #convert startdate to date type
    
    print('checking... {0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy\n'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) 
    if os.path.exists('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))) \
        and os.path.exists('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_info.txt'\
          .format(save_dir_name, proc_start_date, proc_end_date, 
          str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution))):
            
        print('='*80)
        write_log(log_file, '{8} ::: {0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7} files are already exist'\
                  .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution), datetime.now()))
        return 0
    
    else : 
        processing_log += '#Llon =' + str(Llon) + '\n' \
        + '#Rlon =' + str(Rlon) + '\n' \
        + '#Slat =' + str(Slat) + '\n' \
        + '#Nlat =' + str(Nlat) + '\n' \
        + '#resolution =' + str(resolution) + '\n'

        print('{0}-{1} Start making grid arrays...\n'\
              .format(proc_start_date, proc_end_date))
        ni = np.int((Rlon-Llon)/resolution+1.00)
        nj = np.int((Nlat-Slat)/resolution+1.00)
        lon_array = []
        lat_array = []
        data_array = []
        for i in range(ni):
            lon_line = []
            lat_line = []
            data_line = []
            for j in range(nj):
                lon_line.append(Llon+resolution*i)
                lat_line.append(Nlat-resolution*j)
                data_line.append([])
            lon_array.append(lon_line)
            lat_array.append(lat_line)
            data_array.append(data_line)
        lat_array = np.array(lat_array)
        lon_array = np.array(lon_array)
        print('grid arrays are created...........\n')

        total_data_cnt = 0
        file_no=0
        processing_log += '#processing file list\n'
        processing_log += '#No, data_count, filename \n'

        result_array = np.zeros((1, 1, 1))
        for fullname in sorted(glob(os.path.join(dir_name, '*.hdf'))):
            
            result_array = data_array
            file_date = filename_to_datetime(fullname)
            #print('fileinfo', file_date)

            if file_date >= start_date \
                and file_date < end_date :

                try:
                    print('reading file {0}\n'.format(fullname))
                    hdf = SD(fullname, SDC.READ)
                    # Read AOD dataset.
                    DATAFIELD_NAME = 'Optical_Depth_Land_And_Ocean'
                    hdf_raw = hdf.select(DATAFIELD_NAME)
                    hdf_data = hdf_raw[:,:]
                    scale_factor = hdf_raw.attributes()['scale_factor']
                    offset = hdf_raw.attributes()['add_offset']
                    hdf_value = hdf_data * scale_factor + offset
                    hdf_value[hdf_value < 0] = np.nan
                    hdf_value = np.asarray(hdf_value)

                    # Read geolocation dataset.
                    lat = hdf.select('Latitude')
                    latitude = lat[:,:]
                    lon = hdf.select('Longitude')
                    longitude = lon[:,:]
                except Exception as err :
                    print("Something got wrecked \n")
                    write_log(err_log_file, '{2} ::: {0} with {1}'\
                      .format(err, fullname, datetime.now()))
                    continue

                if np.shape(longitude) != np.shape(latitude) or np.shape(latitude) != np.shape(hdf_value) :
                    print('data shape is different!! \n')
                    print('='*80)
                else :
                    lon_cood = np.array(((longitude-Llon)/resolution*100//100), dtype=np.uint16)
                    lat_cood = np.array(((Nlat-latitude)/resolution*100//100), dtype=np.uint16)
                    data_cnt = 0
                    for i in range(np.shape(lon_cood)[0]) :
                        for j in range(np.shape(lon_cood)[1]) :
                            if int(lon_cood[i][j]) < np.shape(lon_array)[0] \
                                and int(lat_cood[i][j]) < np.shape(lon_array)[1] \
                                and not np.isnan(hdf_value[i][j]) :
                                data_cnt += 1 #for debug
                                result_array[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                    file_no += 1
                    total_data_cnt += data_cnt
                processing_log += str(file_no) + ',' + str(data_cnt) +',' + str(fullname) + '\n'
                print(thread_number,  proc_date[0], 'number of files: ', 
                      file_no, 'tatal data cnt :' , data_cnt)
        processing_log += '#total data number =' + str(total_data_cnt) + '\n'

        np.save('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_result.npy'\
                .format(save_dir_name, proc_start_date, proc_end_date, 
                str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), result_array)
        
        with open('{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7}_info.txt'\
                  .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)), 'w') as f:
            f.write(processing_log)
        print('#'*60)
        write_log(log_file, '{0}AOD_3K_{1}_{2}_{3}_{4}_{5}_{6}_{7} files are is created.'\
              .format(save_dir_name, proc_start_date, proc_end_date, 
                  str(Llon), str(Rlon), str(Slat), str(Nlat), str(resolution)))

    return 0 # Return a dummy value
    # Putting large values in Queue was slow than expected(~10min)
    #return result_array, processing_log



def read_MODIS_hdf_and_make_statistics_array(dir_name, DATAFIELD_NAME, proc_date, 
                             resolution, Llon, Rlon, Slat, Nlat):
    
    proc_start_date = proc_date[0]
    proc_end_date = proc_date[1]
    thread_number = proc_date[2]
    processing_log = '#This file is created using python \n' \
                '#https://github.com/guitar79/MODIS_AOD \n' \
                + '#start date = ' + str(proc_date[0]) +'\n'\
                + '#end date = ' + str(proc_date[1]) +'\n'
    
    #convert start_date and end_date to date type
    start_date = datetime(int(proc_start_date[:4]), 
                          int(proc_start_date[4:6]), 
                          int(proc_start_date[6:8])) 
    end_date = datetime(int(proc_end_date[:4]), 
                        int(proc_end_date[4:6]), 
                        int(proc_end_date[6:8]))
    
    processing_log += '#Llon =' + str(Llon) + '\n' \
    + '#Rlon =' + str(Rlon) + '\n' \
    + '#Slat =' + str(Slat) + '\n' \
    + '#Nlat =' + str(Nlat) + '\n' \
    + '#resolution =' + str(resolution) + '\n'

    print("{0}-{1} Start making grid arrays...\n".format(proc_start_date, proc_end_date))
    ni = np.int((Rlon-Llon)/resolution+1.00)
    nj = np.int((Nlat-Slat)/resolution+1.00)
    lon_array = []
    lat_array = []
    data_array = []
    for i in range(ni):
        lon_line = []
        lat_line = []
        data_line = []
        for j in range(nj):
            lon_line.append(Llon+resolution*i)
            lat_line.append(Nlat-resolution*j)
            data_line.append([])
        lon_array.append(lon_line)
        lat_array.append(lat_line)
        data_array.append(data_line)
    lat_array = np.array(lat_array)
    lon_array = np.array(lon_array)
    print('Grid arrays are created...........\n')

    total_data_cnt = 0
    file_no = 0
    processing_log += '#processing file list\n'
    processing_log += '#No, data_count, filename \n'

    result_array = np.zeros((1, 1, 1))
    fullnames = sorted(glob(os.path.join(dir_name, '*.hdf')))
    if not fullnames  :
        for fullname in fullnames:
            result_array = data_array
            file_date = filename_to_datetime(fullname)
            #print('fileinfo', file_date)
    
            if file_date >= start_date \
                and file_date < end_date :
    
                try:
                    print('reading file {0}\n'.format(fullname))
                    hdf = SD(fullname, SDC.READ)
                    # Read AOD dataset.
                    hdf_raw = hdf.select(DATAFIELD_NAME)
                    hdf_data = hdf_raw[:,:]
                    scale_factor = hdf_raw.attributes()['scale_factor']
                    offset = hdf_raw.attributes()['add_offset']
                    hdf_value = hdf_data * scale_factor + offset
                    hdf_value[hdf_value < 0] = np.nan
                    hdf_value = np.asarray(hdf_value)
    
                    # Read geolocation dataset.
                    lat = hdf.select('Latitude')
                    latitude = lat[:,:]
                    lon = hdf.select('Longitude')
                    longitude = lon[:,:]
                except Exception as err :
                    print("Something got wrecked : {}".format(err))
    
                    continue
    
                if np.shape(longitude) != np.shape(latitude) or np.shape(latitude) != np.shape(hdf_value) :
                    print('data shape is different!! \n')
                    print('='*80)
                else :
                    lon_cood = np.array(((longitude-Llon)/resolution*100//100), dtype=np.uint16)
                    lat_cood = np.array(((Nlat-latitude)/resolution*100//100), dtype=np.uint16)
                    data_cnt = 0
                    for i in range(np.shape(lon_cood)[0]) :
                        for j in range(np.shape(lon_cood)[1]) :
                            if int(lon_cood[i][j]) < np.shape(lon_array)[0] \
                                and int(lat_cood[i][j]) < np.shape(lon_array)[1] \
                                and not np.isnan(hdf_value[i][j]) :
                                data_cnt += 1 #for debug
                                result_array[int(lon_cood[i][j])][int(lat_cood[i][j])].append(hdf_value[i][j])
                    file_no += 1
                    total_data_cnt += data_cnt
                processing_log += str(file_no) + ',' + str(data_cnt) +',' + str(fullname) + '\n'
                print(thread_number,  proc_date[0], 'number of files: ', 
                      file_no, 'tatal data cnt :' , data_cnt)
        processing_log += '#total data number =' + str(total_data_cnt) + '\n'        
    
    else  :
        print("No file exist...")

    return result_array, processing_log



