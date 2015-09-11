import csv
import datetime
import os
import fnmatch
import sys
import pymssql
import os.path
from itertools import islice
import math
import numpy as np
import matplotlib.pyplot as plt, mpld3
import matplotlib 
import collections
import os
import errno
from scipy.interpolate import interp1d

"""
A script for correction of streamflow.
Version 1.0.2
Fox Sparky Peterson
Creative Commons ShareAlike 3.0 License
You are free to share, copy, transmit, and adapt this work, but you must provide attribution to Fox Peterson and ShareAlike in kind.
"""

def make_sure_path_exists(path):
    """ 
    stack overflow says this is a cross platform solution
    os.umask sets the permissions on the directory to all openness, so that you can write the new images within the files you create
    if that seems to not work, you will want to change the name of the directory each run so you can fix this; I'm not sure how your computer will respond.
    """
    try:
        os.umask(0000)
        os.mkdir(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def string_correct(sitecode_raw, wateryear_raw):
    """ make sure the cases are correct """
    sitecode = sitecode_raw.upper()
    wateryear = int(wateryear_raw)

    return sitecode, wateryear

def find_files(sitecode, wateryear, subfolder):
    """ find the raw data files"""
    for root, dir, names in os.walk(subfolder):
        for x in names:
            if sitecode in x and str(wateryear) in x:
                return os.path.join(subfolder,x)
            else:
                continue


def create_subfolders(sitecode, wateryear):
    """ create directories for images and for working data"""

    # directory of images; path to images with a slash in case
    dir_images = str(sitecode) + "_" + str(wateryear) + "_" + "images"
    
    # directory of working data; where the 7 digit data lives
    dir_working = str(sitecode) + "_" + str(wateryear) + "_" + "working"

    # create a directory for images if it does not already exist in your folder
    try:
        make_sure_path_exists(dir_images)
    except Exception:
        print Exception

def getcorrtable(wateryear,sitecode):
    """ corr table must be present and named corr_table_wsname_year.csv 
    for example, corr_table_gscc01_2014.csv
    corr_table_gsws01_2015.csv
    etc.
    """

    if wateryear == "2014" or wateryear == 2014:
        corr = "corr_table_" + sitecode.lower()+"_"+"2014.csv"
    elif wateryear == "2015" or wateryear == 2015:
        corr = "corr_table_" + sitecode.lower()+"_"+"2015.csv"
    elif wateryear == "2013" or wateryear == 2013:
        corr = "corr_table_" + sitecode.lower()+"_"+"2013.csv"
    elif wateryear == "2012" or wateryear == 2012:
        corr = "corr_table_" + sitecode.lower()+"_"+"2012.csv"
    elif wateryear == "2011" or wateryear == 2011:
        corr = "corr_table_" + sitecode.lower()+"_"+"2011.csv"
    elif wateryear == "2010" or wateryear == 2010:
        corr = "corr_table_" + sitecode.lower()+"_"+"2010.csv"
    else:
        print("using a default corr table....")
        corr = "corr_table_2014.csv"

    return corr

def convert_corr_to_dict(corr, sitecode):
    """ this was the first time I ever used a dictionary explicitly.
    dateformat_ideal is what the db has
    dateformat_old is what craig enters
    dateformat_13char is the 13 character date
    """

    dateformat_ideal = '%Y-%m-%d %H:%M:%S'
    dateformat_old = '%m/%d/%Y %H:%M'
    dateformat_13char = '%Y%m%d %H%M'

    reader = csv.reader(open(corr))
    
    d = {}

    for row in reader:
        
        if row[2] == sitecode:
            try:
                dt = datetime.datetime.strptime(row[3], dateformat_ideal)
            except Exception:
                try:
                    dt = datetime.datetime.strptime(row[3], dateformat_old)
                except Exception:
                    dt = datetime.datetime.strptime(row[3], dateformat_13char)

            dt = dt-datetime.timedelta(minutes = dt.minute % 5)
            key = str(dt)
            bgncr = float(row[4])
            bgnhg = float(row[5])
            enddt = str(row[6])
            comm = str(row[9])
            
            try:
                endcr = float(row[7])
                endhg = float(row[8])
            
            except Exception as exc:
                endcr = row[7]
                endhg = row[8]
        else:
            continue
        
        # if the key is already in the dictionary, skip it
        if key in d:
            continue

        # the items surrounded by 7 to 9
        d[key] = [sitecode, bgncr, bgnhg, enddt, endcr, endhg, comm]

        od = collections.OrderedDict(sorted(d.items()))

    # return the dictionary to the main loop!
    return od

def drange(start, stop, step):
    """ fraction/date range generator """
    r = start
    while r < stop:
        yield r
        r += step

def generate_first(sitecode, wateryear, filename, corr_od, sparse=False):
    """ 
    generates the outputs without if sparse is set to false and with them if true
    """

    # "output dictionary" --> anytime I use od in a program this is what it is -- Fox 09/10/2015
    od = {}

    # figure out which column contains the date and what its type is
    date_type, column = test_csv_structure(filename)

    # opent the file and process
    with open(filename, 'rb') as readfile:
        reader = csv.reader(readfile)

        for row in reader:
            dt = datetime.datetime.strptime(str(row[column]), date_type)

            print row
            
def test_csv_date(filename, date_column):
    """" figure out what date format to use"""

    dateformat_ideal = '%Y-%m-%d %H:%M:%S'
    dateformat_old = '%m/%d/%y %H:%M'
    dateformat_13char = '%Y%m%d %H%M'
    
    with open(filename, 'rb') as readfile:
        reader = csv.reader(readfile)
        testline = reader.next()

        try:
            is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_ideal)
            return dateformat_ideal
        except Exception:
            try:
                is_a_date = datetime.datetime.stprtime(str(testline[date_column]), dateformat_old)
                return dateformat_old
            except Exception:
                try:
                    is_a_date = datetime.datetime.strptime(str(testline[date_column]), dateformat_13char)
                    return dateformat_13char
                except Exception:
                    return False

def test_csv_structure(filename):
    """ try to find the date column in about 7 columns"""
    for column in [0,1,2,3,4,5,6,7]:
    
        date_type = test_csv_date(filename, column)
        if date_type != False:
            return date_type, column
        else:
            continue

if __name__ == "__main__":

    sitecode_raw = sys.argv[1]
    wateryear_raw = sys.argv[2]
    method= sys.argv[3]

    sitecode, wateryear = string_correct(sitecode_raw,  wateryear_raw)
    
    # get the corr table and put it into a dictionary
    corr_name = getcorrtable(wateryear, sitecode)
    corr = os.path.join('corr_table', corr_name)
    corr_od = convert_corr_to_dict(corr, sitecode)

    if method == "first":
        create_subfolders(sitecode, wateryear)
        filename = find_files(sitecode, wateryear, 'raw_data')
        print filename
    elif method == "sparse":
        pass
    elif method == "re":
        pass

    print filename
    generate_first(sitecode, wateryear, filename, corr_od, sparse=False)
    