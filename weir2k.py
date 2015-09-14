import csv
import datetime
import os
import fnmatch
import sys
import shutil
import pymssql
import os.path
from itertools import islice
import math
import numpy as np
import matplotlib.pyplot as plt 
import mpld3
import matplotlib.dates as mdates
import matplotlib 
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

    # directory for backups; because I am paranoid
    dir_backup = str(sitecode) + "_" + str(wateryear) + "_" + "backups"

    # create a directory for images if it does not already exist in your folder
    try:
        make_sure_path_exists(dir_images)
    except Exception:
        print Exception

    # create a directory for working files if it's not in your folder
    try:
        make_sure_path_exists(dir_working)
    except Exception:
        print Exception

    # create a directory for backup files if it's not in your folder
    try:
        make_sure_path_exists(dir_backup)
    except Exception:
        print Exception
    
def convert_corr_to_dict(sitecode):
    """ this was the first time I ever used a dictionary explicitly.
    dateformat_ideal is what the db has
    dateformat_old is what craig enters
    dateformat_13char is the 13 character date
    """

    corr_name = "corr_table_" + sitecode.lower() + "_" + str(wateryear) + ".csv"
    corr = os.path.join('corr_table', corr_name)

    dateformat_ideal = '%Y-%m-%d %H:%M:%S'
    dateformat_old = '%m/%d/%Y %H:%M'
    dateformat_13char = '%Y%m%d %H%M'

    # output
    od = {}

    # fox changed this on 09-13-2015 --> I think this is a better syntax but I don't know why
    with open(corr, 'rb') as readfile:
        reader = csv.reader(readfile)

        # no need to bring in any values that begin after this water year
        test_value = datetime.datetime(wateryear,10,1,0,5)
    
        for row in reader:
        
            # skip header lines
            if str(row[2]) != sitecode:
                continue

            try:
                dt = datetime.datetime.strptime(str(row[3]), dateformat_ideal)

                if dt.minute % 5 != 0:
                    new_minute = dt.minute // 5 * 5 
                    dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                    dt += datetime.timedelta(minutes = 5)
                    # if the beginning date time from the corr table is bigger than the last day of the water year, we won't ever use this correction, so don't bother to import it. 
                    if dt >=test_value:
                        return od
        
            except Exception:
                
                try:
                    dt = datetime.datetime.strptime(str(row[3]), dateformat_old)
                    if dt.minute % 5 != 0:
                        new_minute = dt.minute // 5 * 5 
                        dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                        dt += datetime.timedelta(minutes = 5)
                       
                        # see note above
                        if dt >=test_value:
                            return od

                except Exception:
                    try:
                        dt = datetime.datetime.strptime(str(row[3]), dateformat_13char)
                        # set the correction to occur on the last five minute interval 
                        if dt.minute % 5 != 0:
                            new_minute = dt.minute // 5 * 5 
                            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, new_minute, 0)
                            dt += datetime.timedelta(minutes = 5)
                            if dt >=test_value:
                                return od
                
                    except Exception:
                        
                        print "error importing corr table due to incompatible date on the begin date. check the format and try again"
                        dt = None

            bgncr = float(row[4])
            bgnhg = float(row[5])
            bgnratio = bgnhg/bgncr
        
            try:
                enddt = datetime.datetime.strptime(str(row[6]), dateformat_old)

                if enddt.minute % 5 != 0:
                    new_minute = enddt.minute // 5 * 5 
                    enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                    enddt += datetime.timedelta(minutes = 5)
                
            except Exception as exc:

                try:
                    enddt = datetime.datetime.strptime(str(row[6]), dateformat_ideal)
                    if enddt.minute % 5 != 0:
                        new_minute = enddt.minute // 5 * 5
                        enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                        enddt += datetime.timedelta(minutes = 5)
                except Exception:
                    try:
                        enddt = datetime.datetime.strptime(str(row[6]), dateformat_13char)
                        
                        if enddt.minute % 5 != 0:
                            new_minute = enddt.minute // 5 * 5 
                            enddt = datetime.datetime(enddt.year, enddt.month, enddt.day, enddt.hour, new_minute, 0)
                            enddt += datetime.timedelta(minutes = 5)
                    
                    except Exception:
                        
                        print "There is an error importing corr table due to incompatible date on end date - can you bring over an extra record from the subsequent table? In the meantime, I\'ll pass in a None and your adjustments will stop on the last known good correction date"
                        enddt = None
            
            try:
                endcr = float(row[7])
                endhg = float(row[8])            
                endratio = endhg/endcr

            except Exception as exc:
                endcr = None
                endhg = None
                endratio = None

            try:
                # compute the duration of the interval from that beginning time to its follower in minutes
                duration = (enddt - dt).days*1440 + (enddt - dt).seconds//60
            except Exception:
                duration = None

                
            # if the key is already in the dictionary, skip it
            if enddt not in od:
                # populate it
                od[enddt] = {'sitecode': sitecode, 'bgn_cr' : bgncr, 'bgn_hg' :bgnhg, 'bgn_rat': bgnratio, 'bgn_dt' : dt, 'end_cr' : endcr, 'end_hg': endhg, 'end_rat': endratio, 'duration':duration}
            
            elif enddt in od:
                pass
        
    # return the correction table as dictioanry
    return od

def drange(start, stop, step):
    """ fraction/date range generator """
    r = start
    while r < stop:
        yield r
        r += step

def parameterize_first(sitecode, wateryear, filename):
    """ from the raw input figure out which column has the dates and what its format is. assume that the data is in the column which is to the right of the dates. """

    # "output dictionary" --> anytime I use od in a program this is what it is -- Fox 09/10/2015
    od = {}

    # figure out which column contains the date and what its type is
    date_type, column = test_csv_structure(filename)

    # opent the file and process
    with open(filename, 'rb') as readfile:
        reader = csv.reader(readfile)

        for row in reader:
            dt = datetime.datetime.strptime(str(row[column]), date_type)

            try:
                data_value = round(float(row[column + 1]),3)

                if str(data_value) == "nan":
                    data_value = None
                else:
                    pass
            except Exception:
                data_value = None

            # break out of the loop if you have done more than the water year
            if dt > datetime.datetime(wateryear, 10, 1, 0, 0):
                return od
            else:
                pass
            
            if dt not in od:
                od[dt] = data_value
            elif dt in od:
                pass
    
    return od

def generate_first(od, sparse=False):
    """ 
    generates the outputs with estimations if sparse is set to false and without if true
    The "first" output will not show the adjustments, just the site code, date, data, and estimated data if you set sparse to false
    """

    output_filename = sitecode + "_" + str(wateryear) + "_" + "first.csv"

    if sparse == False:
        
        # one perfect wateryear from 2013-10-01 00:00:00 to 2014-10-01 00:00:00 - iterator always "stops" one shy of last date time
        compare_range = drange(datetime.datetime(wateryear-1, 10, 1, 0, 0), datetime.datetime(wateryear, 10, 1, 0, 5), datetime.timedelta(minutes=5))

        # create a blank dictionary with 5 minute spacing
        blank_dict = dict.fromkeys(compare_range)

        # update your blank dictionary it with existing values from the raw data
        # anything that doesn't have a value will be None
        blank_dict.update(od)

        # create another dictionary to contain flags associated with those estimations
        flag_dict = {}

        # first fill it with blanks and accepteds based on the blanks!
        for each_date in blank_dict.keys():
            if blank_dict[each_date] == None:
                flag_dict.update({each_date:'M'})
            else:
                flag_dict.update({each_date:'A'})

        # create a dictionary to contain estimations
        estim_dict = {}
        
        # a list of the observed dates in the raw data
        list_obs = sorted(od.keys())

        # iterate over the observed dates in the raw data
        for index,each_obs in enumerate(list_obs[:-1]):
            
            # compute the difference between subsequent observations and test if it is 5 minutes
            compute_obs = list_obs[index+1] - list_obs[index]
            """
            >>> a = datetime.datetime(2010,10,1,0,0) - datetime.datetime(2010,9,29,0,0)
            >>> b = datetime.timedelta(minutes = 1440*2)
            >>> a == b
            >>> True
            """
            # if the obsevations computed are five minutes from one another, store them in the estimated dictionary, otherwise, use the drange function to do a linear interpolation between them
            if compute_obs == datetime.timedelta(minutes=5):
                # the datetime : the value at that date time
                estim_dict.update({list_obs[index]:od[list_obs[index]]})

            else:
                #import pdb; pdb.set_trace()
                # generate a small range of dates for the missing dates and listify
                mini_dates = drange(list_obs[index], list_obs[index+1], datetime.timedelta(minutes=5))
                dl = [x for x in mini_dates]
                
                # if the current value and the next one are the same
                if od[list_obs[index]] == od[list_obs[index+1]]:
                    vl = [od[list_obs[index]]]*len(dl)
                    el = 'E'*len(vl)
                    # update the estimations dictionary with these new values
                    newd = dict(zip(dl,vl))
                    # update the flags with "E"
                    newd2 = dict(zip(dl,el))
                    # update the estimations dictionary
                    estim_dict.update(newd)
                    flag_dict.update(newd2)

                else:
                    # a numpy array for the number of missing
                    indices_missing = np.arange(len(dl))
                    knownx = [indices_missing[0], indices_missing[-1]]
                    knowny = [od[list_obs[index]], od[list_obs[index+1]]]
                    # interpolation function
                    fx = interp1d(knownx, knowny)
                    # apply to the indices
                    vl = fx(indices_missing)
                    # estimate code for the length of vl
                    el = 'E'*len(vl)
                    # update the estimations dictionary with these new values
                    newd = dict(zip(dl,vl))
                    # update the flags with "E"
                    newd2 = dict(zip(dl,el))
                    estim_dict.update(newd)
                    flag_dict.update(newd2)

                    newd={}
                    newd2={}

        # write it to a csv file for subsequent generation
        with open(output_filename, 'wb') as writefile:
            writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

            try:
                # blank dict has been gap filled
                for each_date in sorted(blank_dict.keys()):
                
                    dt = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')
                    writer.writerow([sitecode, dt, blank_dict[each_date], estim_dict[each_date], flag_dict[each_date]])

            except Exception:

                pass

    elif sparse == True:

        # a list of the observed dates in the raw data
        list_obs = sorted(od.keys())

        # write it to a csv file for subsequent generation
        with open(output_filename, 'wb') as writefile:
            writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

            try:
                # blank dict has been gap filled
                for each_date in list_obs:
                
                    dt = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')
                    writer.writerow([sitecode, dt, od[each_date], od[each_date], 'A'])

            except Exception:
                pass
    return output_filename

def do_adjustments(sitecode, wateryear, filename, corr_od, method):
    """ 
    performs adjustments on the outputs - ALWAYS pulls from column 3!
    """

    output_filename = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working",sitecode + "_" + str(wateryear) + "_" + "re.csv")

    # create a backup copy if you're doing the re-adjustment, in the chance something got messed up
    if method=="re":
        shutil.copy(output_filename, os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "backups",sitecode + "_" + str(wateryear) + "_" + "re.csv"))
    else:
        pass

    od = {}
    
    # check date type by using the first column
    date_type = test_csv_date(filename, 1)
    
    # open the input file and process
    with open(filename, 'rb') as readfile:
        reader = csv.reader(readfile)

        for row in reader:
            
            # don't bother carrying site code, we'll have it in the function
            dt = datetime.datetime.strptime(str(row[1]), date_type)
            
            # in both the first and "re" it is in column 3
            try:
                data_value = round(float(row[3]),3)
            except Exception:
                data_value = None

            
            # raw values brought across but don't do anything with them
            try:
                raw_value = round(float(row[2]),3)
            except Exception:
                raw_value = None
            
            if method != "re":
                # flag values are carried across but again, don't do anything with them
                flag_value = str(row[4])
            elif method == "re":
                flag_value = str(row[5])

            if dt not in od:
                
                # assign 'NA' for events beforehand
                od[dt] = {'raw' : raw_value, 'val': data_value, 'fval': flag_value, 'event':'NA'}
            
            elif dt in od:
                pass
       
        # the key function is "determine weights"
        wd = determine_weights(sitecode, wateryear, corr_od, od)
        """
        This monstrous structure contains all of your wildest linear interpolation dreams for validating the code!

        wd[each_date] = {'val': od[each_date]['val'], 'adj': round(adjusted_value,3), 'wt_bgn': round(time_difference/corr_od[this_correction]['duration'],3), 'wt_end': round((1-time_difference/corr_od[this_correction]['duration']),3), 'wt_bgn_ratio': round(weighted_begin_ratio,3), 'wt_end_ratio': round(weighted_end_ratio,3), 'raw' : od[each_date]['raw'], 'fval': od[each_date]['fval'], 'event': event}

        """
    with open(output_filename, 'wb') as writefile:
        writer = csv.writer(writefile, delimiter = ",", quoting=csv.QUOTE_NONNUMERIC)

        for each_date in sorted(wd.keys()):
            writer.writerow([sitecode, datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S'), wd[each_date]['raw'], wd[each_date]['val'], wd[each_date]['adj'], wd[each_date]['fval'], wd[each_date]['event']])

    return wd, output_filename

def determine_weights(sitecode, wateryear, corr_od, od):
    """ The corr dates prior to the start of the data set can be disregarded except for the one just prior to the start"""

    try:
        corr_dates_as_list = sorted(corr_od.keys())
    except Exception:
        # in 2015 the end date is missing so we need to not use that one, it is "None"
        corr_dates_1 = [x for x in corr_od.keys() if x != None]
        corr_dates_as_list = sorted(corr_dates_1)
    observed_dates_as_list = sorted(od.keys())

    # filter the correction table to only include things that are indexed on an end date which is in our water year - nothing after this year.
    relevant_corr_dates = [x for x in corr_dates_as_list if x >= datetime.datetime(wateryear-1, 10,1,0,0)]
    
    # working dictionary
    wd = {}

    # we'll use the same "correction" until we pass that time, at which point, we'll move to the next correction factor, by calling the iterator.next() method
    iterator_for_correction = iter(relevant_corr_dates)

    # the first correction to be applied
    this_correction = iterator_for_correction.next()

    for each_date in observed_dates_as_list:

        # as long as the date is less than the correction factor or equal to it
        if each_date <= this_correction:

            # the time left until the end of the interval, in minutes
            time_difference = float((this_correction-each_date).days*1440 + (this_correction - each_date).seconds//60)
            
            """
            "weighted ratios" of each time : as we move towards the adjustment (this_correction) time, the ending ratio of hg/cr becomes more influential as compared to the beginning ratio of the hg/cr because the time_difference to the end gets very small relative to the duration of the interval as a whole. on the actual correction moment, there is no longer any beginning influence at all, as the difference between the current moment and the end of the interval are the same. the cr then is mapping directly onto the hook gage - the value in (cr) times the ratio of the (hg/cr) at the end times 1 yields the hg, and the ratio of the (hr/cr) at the beginning is times 0, so that does not have an affect.
            """

            weighted_begin_ratio = corr_od[this_correction]['bgn_rat']*time_difference/corr_od[this_correction]['duration']
            
            weighted_end_ratio = corr_od[this_correction]['end_rat']*(1-time_difference/corr_od[this_correction]['duration']) 

            try:
                adjusted_value = round(weighted_begin_ratio*od[each_date]['val'] + weighted_end_ratio*od[each_date]['val'],3)
            except Exception:
                adjusted_value = None

            if each_date != this_correction:
                event = 'NA'
            else: 
                event = "MAINTE"

        elif each_date > this_correction:
            
            # assign the event
            event = 'NA'

            try:
                # step to the next correction factor
                this_correction = iterator_for_correction.next()
            except StopIteration:
                return wd

            # you still need to compute this value here! because the correction has moved on it should fall into the less than pool on the next loop

            # the time left until the end of the interval, in minutes
            time_difference = float((this_correction - each_date).days*1440 + (this_correction - each_date).seconds//60)
            
            # the "weighted ratios" of that time 
            # as we move towards the adjustment, the ending ratio of hg/cr becomes more influential because the time_difference to the end gets really small and the duration is the same. when you are at the end, there is no longer any beginning influence and you are doing all the adjustment of the hg. when you move into the next interval, then, you are aligned.
            weighted_begin_ratio = corr_od[this_correction]['bgn_rat']*time_difference/corr_od[this_correction]['duration']
            
            weighted_end_ratio = corr_od[this_correction]['end_rat']*(1-time_difference/corr_od[this_correction]['duration']) 

            try: 
                adjusted_value = round(weighted_begin_ratio*od[each_date]['val'] + weighted_end_ratio*od[each_date]['val'],3)
            except Exception:
                adjusted_value = None

        if each_date not in wd:


            wd[each_date] = {'val': od[each_date]['val'], 'adj': adjusted_value, 'wt_bgn': round(time_difference/corr_od[this_correction]['duration'],3), 'wt_end': round((1-time_difference/corr_od[this_correction]['duration']),3), 'wt_bgn_ratio': round(weighted_begin_ratio,3), 'wt_end_ratio': round(weighted_end_ratio,3), 'raw' : od[each_date]['raw'], 'fval': od[each_date]['fval'], 'event': event}

        elif each_date in wd:
            print "this date has already been put in"
    
    return wd


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

def make_graphs(sitecode, wateryear, adjusted_dictionary):
    """ make the graphs as you did before"""

    # directory of images; path to images with a slash in case
    dir_images = str(sitecode) + "_" + str(wateryear) + "_" + "images"

    # no sense in sorting this a million times
    sorted_dates = sorted(adjusted_dictionary.keys())

    for each_month in xrange(1,12):
        
        if each_month not in [10, 11, 12]:
            dates = [x for x in sorted_dates if x.month == each_month and x.year==wateryear]
            
            prior_values = [adjusted_dictionary[x]['val'] for x in dates if adjusted_dictionary[x]['val'] != None]
            pvd = [x for x in dates if adjusted_dictionary[x]['val'] != None]
            
            adjusted_values = [adjusted_dictionary[x]['adj'] for x in dates if adjusted_dictionary[x]['adj'] != None]
            avd = [x for x in dates if adjusted_dictionary[x]['adj'] != None]

            #maintes = [adjusted_dictionary[x]['adj'] for x in dates if adjusted_dictionary[x]['event'] == "MAINTE"]
            #mainte_dates = [x for x in dates if adjusted_dictionary[x]['event'] == "MAINTE"]

            image_name = str(wateryear) + "_" + str(each_month) + "_wy_" + sitecode + ".png"
            name1 = os.path.join(dir_images, image_name)

            html_image_name = str(wateryear) + "_" + str(each_month) + "_wy_" + sitecode + ".html"
            name2 = os.path.join(dir_images, html_image_name)

            fig, ax = plt.subplots()
            fig.autofmt_xdate()
            ax.fmt_xdata = mdates.DateFormatter('%Y-%m')
            ax.plot(pvd, prior_values, color = 'blue', linewidth= 1.2, alpha = 0.5, label = 'corrected cr logger')
            ax.plot(avd, adjusted_values, color = 'red', linewidth= 0.7, label = 'adjusted to hg')
            #ax.scatter(mainte_dates, maintes, s=30, c='red', alpha = 0.4, label='MAINTE')
            ax.legend(loc = 1)
            plt.savefig(name1)

            html = mpld3.fig_to_html(fig)
            mpld3.save_html(fig, name2)

            plt.close()


        elif each_month in [10,11,12]:
            dates = [x for x in sorted_dates if x.month == each_month and x.year == (wateryear -1)]
            prior_values = [adjusted_dictionary[x]['val'] for x in dates if adjusted_dictionary[x]['val'] != None]
            pvd = [x for x in dates if adjusted_dictionary[x]['val'] != None]
            
            adjusted_values = [adjusted_dictionary[x]['adj'] for x in dates if adjusted_dictionary[x]['adj'] != None]
            avd = [x for x in dates if adjusted_dictionary[x]['adj'] != None]
            
            #maintes = [adjusted_dictionary[x]['adj'] for x in dates if adjusted_dictionary[x]['event'] == "MAINTE"]
            #mainte_dates = [x for x in dates if adjusted_dictionary[x]['event'] == "MAINTE"]

            image_name = str(wateryear-1) + "_" + str(each_month) + "_wy_" + sitecode + ".png"
            name1 = os.path.join(dir_images, image_name)

            html_image_name = str(wateryear-1) + "_" + str(each_month) + "_wy_" + sitecode + ".html"
            name2 = os.path.join(dir_images, html_image_name)

            fig, ax = plt.subplots()
            fig.autofmt_xdate()
            ax.fmt_xdata = mdates.DateFormatter('%Y-%m')
            ax.plot(pvd, prior_values, color = 'blue', linewidth= 1.2, alpha = 0.5, label = 'corrected cr logger')
            ax.plot(avd, adjusted_values, color = 'red', linewidth= 0.7, label = 'adjusted to hg')
            #ax.scatter(mainte_dates, maintes, s=30, c='red', alpha = 0.4, label='MAINTE')
            ax.legend(loc = 1)
            plt.savefig(name1)

            ## generate HTML for October
            html = mpld3.fig_to_html(fig)
            mpld3.save_html(fig, name2)

            plt.close()

if __name__ == "__main__":

    sitecode_raw = sys.argv[1]
    wateryear_raw = sys.argv[2]
    method= sys.argv[3]

    sitecode, wateryear = string_correct(sitecode_raw,  wateryear_raw)
    
    # get the corr table and put it into a dictionary
    corr_od = convert_corr_to_dict(sitecode)

    # create subfolders for images and working data
    create_subfolders(sitecode, wateryear)

    # for the "first" and "sparse" methods, we'll generate only the four column format
    if method == "first":
        
        filename = find_files(sitecode, wateryear, 'raw_data')
        print "File found for the " + method + " method : " + filename

        # figure out what columns contain the dates and raw values and read in from csv
        od = parameterize_first(sitecode, wateryear, filename)

        # generate a first data with or without estimations
        output_filename_first = generate_first(od,  sparse=False)

        # generate the adjustments data with the extra column
        adjusted_dictionary, output_filename_re = do_adjustments(sitecode, wateryear, output_filename_first, corr_od, method)

        make_graphs(sitecode, wateryear, adjusted_dictionary)
    
    elif method == "sparse":
        
        filename = find_files(sitecode, wateryear, 'raw_data')
        print "File found for the " + method + " method : " + filename

        # figure out what columns contain the dates and raw values and read in from csv
        od = parameterize_first(sitecode, wateryear, filename)

        # generate a first data with or without estimations
        output_filename_first = generate_first(od,  sparse=True)

        # generate the adjustments data with the extra column
        do_adjustments(sitecode, wateryear, output_filename_first, corr_od, method)

        # generate the adjustments data with the extra column
        adjusted_dictionary, output_filename_re = do_adjustments(sitecode, wateryear, output_filename_first, corr_od, method)


        make_graphs(sitecode, wateryear, adjusted_dictionary)
    elif method == "re":
        output_filename_re = os.path.join(str(sitecode) + "_" + str(wateryear) + "_" + "working",sitecode + "_" + str(wateryear) + "_" + "re.csv")

        adjusted_dictionary, output_filename = do_adjustments(sitecode, wateryear, output_filename_re, corr_od, method)

        make_graphs(sitecode, wateryear, adjusted_dictionary)
    