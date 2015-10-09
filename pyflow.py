#!/usr/bin env python
# -*- coding: utf-8 -*-

import itertools
from scipy.interpolate import interp1d
import numpy as np
import datetime
import csv
import pymssql
import sys
import os

"""
pyFLOW.py is a single file version of all the other flow calculators
The inputs to pyFLOW.py are sitecode, wateryear, "csv"
"""

def fc():
    """ connection to server """

    # Connect to MSSQL Server
    conn = pymssql.connect(server="stewartia.forestry.oregonstate.edu:1433",
                           user="petersonf",
                           password="D0ntd1sATLGA!!",
                           )

    cur = conn.cursor()

    return conn, cur

def get_equation_sets(cur, sitecode, wateryear):
    """
    get the equation set ids to associate with the notch and the actual equation table creation
    ex.
    lookup = {'C1' : '3' : [(start_date1, end_date1), (start_date2, end_date2), (start_date3, end_date3)]}
    """

    od = {}

    # start the collection on january 1 of the prior water year, to make sure we get enough equations
    start_test_DT = datetime.datetime(int(wateryear)-1, 1, 1, 0, 0)
    start_test = datetime.datetime.strftime(start_test_DT, '%Y-%m-%d %H:%M:%S')

    if sitecode not in ['GSWSMA', 'GSWSMF', 'GSCC01', 'GSCC02','GSCC03', 'GSCC04']:

        sql = "SELECT eq_set, eq_ver, eqn_set_code, bgn_date_time, end_date_time FROM fsdbdata.dbo.HF00204 WHERE sitecode like \'" + sitecode +"\' and bgn_date_time > \'" + start_test + "\'"

    elif sitecode in ['GSWSMA', 'GSWSMF', 'GSCC01', 'GSCC02','GSCC03', 'GSCC04']:

        sql = "SELECT eq_set, eq_ver, eqn_set_code, bgn_date_time, end_date_time FROM fsdbdata.dbo.HF00204 WHERE sitecode like \'" + sitecode +"\'"

    cur.execute(sql)

    for row in cur:

        cat_name = str(row[0]) + str(row[1])

        # this tuple represents the dates enclosed by this equation
        #sd_ori = datetime.datetime.strptime(str(row[3]), '%Y-%m-%d %H:%M:%S')
        #round_5 = sd_ori.minute//5*5
        #sd_five = datetime.datetime(sd_ori.year, sd_ori.month, sd_ori.day, sd_ori.hour, round_5)

        #ed_ori = datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S')
        #round_5 = ed_ori.minute//5*5
        #ed_five = datetime.datetime(ed_ori.year, ed_ori.month, ed_ori.day, ed_ori.hour, round_5)

        # add to the output
        if cat_name not in od:
            od[cat_name] = {'tuple_date': [(datetime.datetime.strptime(str(row[3]), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S'))], 'eqn_set': [str(row[2]).lstrip(' ')]}
            #od[cat_name] = {'tuple_date': [(sd_five, ed_five)], 'eqn_set': [str(row[2]).lstrip(' ')]}

        elif cat_name in od:
            od[cat_name]['tuple_date'].append((datetime.datetime.strptime(str(row[3]), '%Y-%m-%d %H:%M:%S'), datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S')))

            #od[cat_name]['tuple_date'].append((sd_five, ed_five))
            od[cat_name]['eqn_set'].append(str(row[2]).lstrip(' '))

    return od


def get_equations_by_value(cur, sitecode, o):
    """
    using the limited to one site code dictionary, get specific equations
    this will modify the structure of "o" -- after this function is done running, both the
    original dictionary and this one will be the same
    I am confident that this works
    """

    sql = "SELECT eq_set, eq_ver, eq_num, ws_acres, max_ht, ln_a, b from fsdbdata.dbo.HF00203 where sitecode like \'" + sitecode + "\' order by max_ht asc"

    cur.execute(sql)

    for row in cur:

        cat_name = str(row[0]) + str(row[1])

        if cat_name not in o:
            continue

        elif cat_name in o:
            #print row

            # if acres aren't listed, update with acres
            if 'acres' not in o:
                o[cat_name].update({'acres': str(row[3])})
            else:
                pass

            # compute the reference lookup
            reference = {round(float(str(row[4])),3) : [round(float(str(row[5])),7), round(float(str(row[6])),7)]}

            if 'eqns' not in o[cat_name]:
                o[cat_name].update({'eqns': reference})

            elif 'eqns' in o[cat_name]:

                if round(float(str(row[4])),3) not in o[cat_name]['eqns']:
                    o[cat_name]['eqns'][round(float(str(row[4])),3)] = [round(float(str(row[5])),7), round(float(str(row[6])),7)]

                elif round(float(str(row[4])),3) in o[cat_name]['eqns']:
                    print(" Error : Same Max Height already listed for this equation set ")

    return o

def get_data_from_sql(cur, sitecode, wateryear):
    """ get data from sql server - used for checking values"""

    # create first and final days for generating the SQL query
    first_day_DT = datetime.datetime(int(wateryear)-1, 9, 30, 23, 55)
    first_day = datetime.datetime.strftime(first_day_DT, '%Y-%m-%d %H:%M:%S')

    last_day_DT = datetime.datetime(int(wateryear), 10, 1, 0, 5)
    last_day = datetime.datetime.strftime(last_day_DT, '%Y-%m-%d %H:%M:%S')

    query = "select DATE_TIME, STAGE, INST_Q, TOTAL_Q_INT, MEAN_Q, MEAN_Q_AREA, INST_Q_AREA from fsdbdata.dbo.HF00401 where SITECODE like \'" + sitecode  + "\' and DATE_TIME >= \'" + first_day + "\' and DATE_TIME <= \'" + last_day + "\' order by DATE_TIME asc"

    conn, cur = fc()
    cur.execute(query)

    # gather this dictionary containing the raw values from the database
    od = {}

    for row in cur:

        dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

        # on the stage change, subtract one second
        if dt.second == 1:
            dt -= datetime.timedelta(seconds=1)

        if dt not in od:
            od[dt] = {'stage': str(row[1]), 'inst_q': str(row[2]), 'total_q_int': str(row[3]), 'mean_q': str(row[4]), 'mean_q_area': str(row[5]), 'inst_q_area' : str(row[6])}

        elif dt in od:
            print "date = > %s is already in the db-- if on a notch event, disregard warning" %(str(row[0]))

    return od

def get_data_from_csv(csvfilename):
    """
    get the data from one of craigs files
    outputs a lookup : {datetime : 'val': 0.2, 'fval' : a, 'event' : na}
    I feel confident this is working...
    """

    od = {}

    with open(csvfilename, 'rb') as readfile:
        reader = csv.reader(readfile)

        for row in reader:

            # import the date from column 1
            dt = datetime.datetime.strptime(str(row[1]), '%Y-%m-%d %H:%M:%S')

            # get the correct value from column 4 -- hg adjusted!
            try:
                val = str(row[4])
            except Exception:
                val = str(row[3])

            if str(val) == "nan":
                val = "None"

            # get the flag from column 5
            try:
                flag = str(row[5])
            except Exception:
                flag = str(row[3])

            # get the event from column 6
            try:
                event = str(row[6])
            except Exception:
                event = str(row[4])

            if dt not in od:
                od[dt] = {'val': val, 'fval': flag, 'event' : event}

            elif dt in od:
                pass

    return od

def set_up_iterators(o2, o1):
    """ Bin the incoming data into the appropriate equation sets
    and create some iterators

    od = {'b1' : 'flags' : <iterator>, 'vals' :<iterator> }
    I am confident that this section is working
    """
    od = {}

    hr_d = sorted(o2.keys())

    first_date = hr_d[0]
    last_date = hr_d[-1]

    for each_set in o1.keys():

        list_of_tuples_sorted = sorted(o1[each_set]['tuple_date'])

        for each_tuple in list_of_tuples_sorted:

            # if the last date of the tuple comes before the data starts, pass it
            if each_tuple[1] <= first_date:
                print "I found a tuple with " + datetime.datetime.strftime(each_tuple[1], '%Y-%m-%d %H:%M:%S') + " as its end date, and this is larger than the first data observation at " + datetime.datetime.strftime(first_date, '%Y-%m-%d %H:%M:%S' ) + ", so I am not using this equation"
                continue

            # if the first date of the tuple occurs after the final date in the data, continue
            if each_tuple[0] > last_date:
                print "I found a tuple that begins on " + datetime.datetime.strftime(each_tuple[0], '%Y-%m-%d %H:%M:%S') + " and this is larger than the final observation in the data on " + datetime.datetime.strftime(last_date, '%Y-%m-%d %H:%M:%S')
                continue

            # if the first date of the tuple is less than the first date of data, begin the use of that equation set with the first date of data;
            # otherwise, if its not but it's also not after the last date, begin with the tuples date
            if each_tuple[0] <= first_date:
                begin_on = first_date
            else:
                begin_on = each_tuple[0]

            if each_tuple[1].year > 2049:
                end_on = last_date+datetime.timedelta(minutes=5)
            else:
                end_on = each_tuple[1]+datetime.timedelta(minutes=5)

            # should not fail even if the "end on" is beyond its range because it is still less than this
            dts = [x for x in hr_d if x >= begin_on and x <= end_on]

            print("Data Found! Under the group of " + each_set + ", which starts on " + datetime.datetime.strftime(begin_on, '%Y-%m-%d %H:%M:%S') + " and ends on " + datetime.datetime.strftime(end_on, '%Y-%m-%d %H:%M:%S'))

            raw_dts = iter(dts)
            raw_hts = iter([o2[x]['val'] for x in dts])

            if each_set not in od:
                od[each_set] = {'raw_dts': [raw_dts], 'raw_hts':[raw_hts]}

            elif each_set in od:
                od[each_set]['raw_dts'].append(raw_dts)
                od[each_set]['raw_hts'].append(raw_hts)

    # for testing -- if you want to make sure you are not missing an interval
    #print("the last day of the second interval:")
    #print(list(od['C1']['raw_dts'][0])[-1])
    #print("the first day of the next interval: ")
    #print(list(od['B1']['raw_dts'][1])[0])
    #print(list(od['B1']['raw_dts'][1][0]))
    #import pdb; pdb.set_trace()
    return od

def get_samples_dates(cur, sitecode, wateryear):
    """ Creates a list of tuple date ranges between the starting date and the ending date - base on the begining date, anything afterward doesn't get to count"""

    startdate = datetime.datetime.strftime(datetime.datetime(int(wateryear)-1,10,1,0,0), '%Y-%m-%d %H:%M:%S')
    enddate = datetime.datetime.strftime(datetime.datetime(int(wateryear),10,1,0,5), '%Y-%m-%d %H:%M:%S' )

    query = "select date_time from fsdbdata.dbo.cf00206 where sitecode like \'" + sitecode + "\' and date_time >= \'" + startdate + "\' and date_time < \'" + enddate + "\' order by date_time asc"

    cur.execute(query)

    # list of tuples containing start and end dates
    Sdate_list = []

    for row in cur:

        dt = datetime.datetime.strptime(str(row[0]), '%Y-%m-%d %H:%M:%S')

        Sdate_list.append(dt)

    # get the first and last dates from the list and then bound them with that water year; put the beginning of wy in position 0 with the start of the list with it; pop the last date and replace with the last date bounded by the water year.
    try:
        first_date = Sdate_list[0]
        last_date = Sdate_list[-1]

    except Exception:
        print "no S-dates are within the range specified"
        return None

    # to the front of the list, add on the starting date
    Sdate_list.insert(0,datetime.datetime.strptime(startdate,'%Y-%m-%d %H:%M:%S'))
    # to the end of the list add on the ending date
    Sdate_list.append(datetime.datetime.strptime(enddate, '%Y-%m-%d %H:%M:%S')-datetime.timedelta(minutes=5))

    print Sdate_list
    return Sdate_list


def loop_over_data(o3, o1):
    """
    a wrapper for the data iterators
    identify the iterators in each key
    identify the set of rating equations associated with that key
    run the flow on that data set
    """

    # final output dictionary
    od_1 = {}

    # each of the tuples, i.e. 'C1', 'B1'
    for each_key in sorted(o3.keys()):

        # if we have 1 time of C1 and 2 times of B1, we should hit the B1 2 x I think...
        raw_dts_1 = o3[each_key]['raw_dts']
        raw_hts_1 = o3[each_key]['raw_hts']

        # will the eq set always be the same? I don't think I know, I'll pass it on
        rating_calib = o1[each_key]['eqns']
        eq_sets = o1[each_key]['eqn_set']

        # for each iterator in the tuple set, i.e. B1 or C1, may only have 1 or 2 index
        for index, value in enumerate(raw_dts_1):
            print "the key processed is " + each_key + "and the index is " + str(index)

            # the equation set name; i.e. "3" or "4" or "2"
            computed_eq_set = eq_sets[index]

            # create an output structure called od_2
            od_2 = flow_the_data(raw_dts_1[index], raw_hts_1[index], rating_calib, desired=300)

            computed_dates = sorted(od_2.keys())
            print "the number of values in this date structure were " + str(len(od_2))

            computed_stages = [od_2[x]['stage'] for x in computed_dates]
            computed_inst_q = [od_2[x]['inst_q'] for x in computed_dates]
            computed_total_q = [od_2[x]['total_q'] for x in computed_dates]
            computed_mean_q = [od_2[x]['mean_q'] for x in computed_dates]

            for index, each_date in enumerate(computed_dates):

                if each_date not in od_1:
                    od_1[each_date] = {'stage': computed_stages[index], 'inst_q': computed_inst_q[index], 'total_q' : computed_total_q[index], 'mean_q': computed_mean_q[index], 'eqn_set' : computed_eq_set}

                elif each_date in od_1:
                    print "this date has already been included in the lookup"

            print("processed " + each_key + str(index))
        print("finished processed " + each_key)

    return od_1


def check_value_versus_keys(rating_calib, value):
    """
    returns the parameters for the log function if checked
    if the function is still okay, you pass over it
    """

    for index, each_value in enumerate(sorted(rating_calib.keys())):

        if value <= each_value:
            try:
                lower_value = each_value[index-1]
            except Exception:
                lower_value = 0
            return lower_value, each_value
        else:
            pass

def interpolate_raw(first_value, second_value, interval_length):
    """ returns appropriate interpolation, usually is 5 minutes"""
    fxn_interp = interp1d([0,interval_length],[first_value, second_value])

    # a np_array containing the interpolated values
    return fxn_interp(xrange(0, interval_length))

def check_interval_length(first_date, second_date, desired=300):
    """
    check to be sure the interval is the correct length
    the default correct length is 5 minutes
    """
    if type(first_date) == str:
        dt1 = datetime.datetime.strptime(first_date,'%Y-%m-%d %H:%M:%S')
    else:
        pass

    if type(second_date) == str:
        dt2 = datetime.datetime.strptime(second_date,'%Y-%m-%d %H:%M:%S')
    else:
        pass

    try:
        dt_diff = second_date - first_date
    except Exception:
        dt_diff = dt2-dt1

    if dt_diff.seconds == 300:
        return 5
    else:
        print "interval is not the right length!"
        print "using " + str(int(dt_diff.seconds)/60)
        return int(dt_diff.seconds)/60

def logfunc(a,b,x):
    """ the transform we need to solve for winters """
    return np.exp(a + b*np.log(x))

def flag_daily_streams(output_5, output_daily):
    """ assign daily flags based on quality of data """

    od = {}
    
    with open(output_5,'rb') as readfile:
        reader = csv.reader(readfile)
        reader.next()
        for row in reader:

            dt = datetime.datetime.strptime(str(row[4]), '%Y-%m-%d %H:%M:%S')
            flag = str(row[13])
            new_dt = datetime.datetime(dt.year, dt.month, dt.day)

            if new_dt not in od:
                od[new_dt] = [flag]
            elif new_dt in od:
                od[new_dt].append(flag)


    od_1 = {}

    with open(output_daily, 'rb') as readfile:
        reader = csv.reader(readfile)
        reader.next()

        for row in reader:
            dt = datetime.datetime.strptime(str(row[4]), '%Y-%m-%d')
            other_stuff = [str(x) for x in row]

            od_1[dt] = other_stuff

    #print od_1

    for each_key in od.keys():

        percent_m = len([x for x in od[each_key] if x == "M"])/len(od[each_key])
        percent_e = len([x for x in od[each_key] if x == "E"])/len(od[each_key])
        percent_q = len([x for x in od[each_key] if x == "Q"])/len(od[each_key])

        if percent_m > 0.2:
            daily_flag = "M"
            od_1[each_key].append(daily_flag)
        elif percent_e > 0.05:
            daily_flag = "E"
        elif percent_q > 0.05:
            daily_flag = "Q"
        elif percent_m + percent_e + percent_q > 0.05:
            daily_flag = "Q"
        else:
            daily_flag = "A"

        #dt = datetime.datetime.strptime(each_key, '%Y-%m-%d')
        od_1[each_key].append(daily_flag)


    daily_file_name = "flagged_" + output_daily
    with open(daily_file_name, "wb") as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

        writer.writerow(["STCODE","FORMAT","SITECODE","WATERYEAR","DATE","MEAN_Q","MAX_Q","MIN_Q","MEAN_Q_AREA","TOTAL_Q_AREA", "DAILY_FLAG"])
        for each_new_key in sorted(od_1.keys()):

            writer.writerow(od_1[each_new_key])

def flow_the_data(raw_dts, raw_hts, rating_calib, desired=300):
    """
    the actual computation occurs here
    the desired interval is 300 seconds, or "5 minutes"
    """

    od = {}

    # initial values - database precision is 6. Since we need to round it out, go to 7.
    # You might have to play with this if the numbers are off a little bit.
    # Not going out enough or too much will throw you into the wrong equation set.
    this_stage = round(float(raw_hts.next()),7)
    print "the first stage is " + str(this_stage)

    this_date = raw_dts.next()
    print "the first date is " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S')

    # Iteration will continue until we run out of values... exception is thrown and results returned
    while True:

        try:
            try:
                low_cutoff, this_max = check_value_versus_keys(rating_calib, this_stage)

            except TypeError:
                import pdb; pdb.set_trace()
                try:
                    low_cutoff, this_max = None, None
                    print "A type error occurred, check if there is a None in the data on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    print "the exception was thrown here and od was returned"
                    import pdb; pdb.set_trace()
                    return od

            try:
                next_stage = round(float(raw_hts.next()),7)
                next_date = raw_dts.next()

            except Exception:
                # iterate over the "next" values that don't work - i.e. you want to pass over the value that doesn't work because it is a None or NaN ...
                #import pdb; pdb.set_trace()
                discard_output1 = raw_hts.next()
                discard_output2 = raw_dts.next()
                print "unexpected next value of " + str(discard_output1) + " for " + datetime.datetime.strftime(discard_output2, '%Y-%m-%d %H:%M:%S')
                print "skipping this value and continuing"

                if str(discard_output1) == "nan":
                    discard_output1 = "None"


                # dealing with when you have an interval that comes off a missing value - if there isn't a stage, all outputs is none -- if there is a stage, give the stage
                if str(discard_output1)=="None":
                    od[discard_output2] = {'stage': None, 'inst_q': None, 'total_q': None, 'mean_q':None}

                else:
                    od[discard_output2] ={'stage': discard_output1, 'inst_q': None, 'total_q': None, 'mean_q': None}

                #import pdb; pdb.set_trace()
                continue

            # makes sure that the interval is the correct length (300 seconds == 5 minutes). if it is not, returns the appropriate length. If you want not five minutes add in a third arguement for a different stamp like: interval_length = check_interval_length(this_date, next_date, desired = 100) or whatever you want
            interval_length = check_interval_length(this_date, next_date)

            # HAPPIEST CASE: if the next stage is the same height as this stage height and they are 5 minutes apart then we can take the calculated value for this height and integrate it over 300 seconds (5 minutes)
            if next_stage == this_stage and interval_length == desired/60:

                #print "desired interval length is " + str(desired/60)

                #print "next stage is this stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S')

                inst_q = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], this_stage)

                # record the values
                if this_date not in od:
                    od[this_date] ={'stage': round(this_stage,3), 'inst_q': inst_q, 'total_q': desired*inst_q, 'mean_q': inst_q}
                else:
                    print "this error should never occur - " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " has already been processed for this site and value!!"

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date
                continue

            # NEXT HAPPIEST CASE: if the next stage height is in the same "bracket" as this stage height and they are five minutes apart then we can do the trapezoid method
            elif next_stage <= this_max and next_stage > low_cutoff and interval_length==desired/60:

                #print "next stage is LIKE stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')

                # current q, q in 5 minutes
                instq_now  = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], this_stage)
                instq_next = logfunc(rating_calib[this_max][0],rating_calib[this_max][1], next_stage)

                # 1/2 * 300 seconds interval  * (base 1 + base 2)
                traps = 0.5*desired*(instq_now + instq_next)

                # record the values
                if this_date not in od:

                    od[this_date] = {'stage': this_stage, 'inst_q': instq_now, 'total_q': traps, 'mean_q': traps/desired}
                else:
                    print "this error should never occur - " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " has already been processed for this site and value!!"

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date
                continue


            # if the next stage > this_max or the next_stage <= the low cutoff or the interval length is not 5
            else:
                try:
                    # if its not in that same range we get a new calibration
                    low_cutoff, this_max = check_value_versus_keys(rating_calib, next_stage)

                except Exception:
                    print "Adam has assigned an unexceptable missing code, converting to Pythonic None"

                    if str(next_stage) == "nan":
                        next_stage = "None"

                    #else:
                    #    next_stage = "None"
                    import pdb; pdb.set_trace()
                    return od

                #print "next stage is UNLIKE stage : " + str(this_stage) + " on " + datetime.datetime.strftime(this_date,'%Y-%m-%d %H:%M:%S')

                # interpolate for one minute for each value
                one_minute_heights = interpolate_raw(this_stage, next_stage, interval_length)

                local_sum = []

                interval_length_seconds = interval_length*60

                # for each one minute height, compute the correct curve
                for each_height in one_minute_heights:

                    _, local_max = check_value_versus_keys(rating_calib, each_height)

                    instq = 60*logfunc(rating_calib[local_max][0], rating_calib[local_max][1], each_height)

                    local_sum.append(instq)

                if this_date not in od:
                    try:
                        od[this_date] = {'stage': round(this_stage,3), 'inst_q': local_sum[0]/60, 'total_q' : sum(local_sum), 'mean_q': sum(local_sum)/interval_length_seconds}

                    except Exception:
                        # strage cases where there is a "one second value" when the code switches
                        test_date = datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S')[-1]

                        if test_date != 0:
                            print "this is a one second measurement on " + datetime.datetime.strftime(this_date, '%Y-%m-%d %H:%M:%S') + " which will go into the next value"
                            pass
                        else:
                            import pdb; pdb.set_trace
                else:
                    print "this is an error"

                # update the "current stage and date"
                this_stage = next_stage
                this_date = next_date

        except StopIteration:
            break

    return od

def drange(start, stop, step):
    """ fraction/date range generator """
    r = start
    while r < stop:
        yield r
        r += step

def quickly_recheck_data(data_in_csv):
    """ call this function if you need to recheck the data for missing values"""

    # initiate with a value of none
    bad = []
    for each_day in sorted(data_in_csv.keys()):
        try:
            valid = float(data_in_csv[each_day])
        except Exception:
            bad.append(each_day)

    if bad != []:
        print "there are bad values on "
        print bad
        import pdb; pdb.set_trace
    else:
        pass

def to_area(sitecode, instq, totalq, meanq):
    """ converts the values to the area"""

    areas = {'GSWS01': 237., 'GSWS02': 149., 'GSWS03': 250., 'GSWS06':32, 'GSWS07':38., 'GSWS08':53., 'GSWS09':21., 'GSWS10':25.3, 'GSWSMA':1436., 'GSWSMF':1436., 'GSCC01':171., 'GSCC02': 169., 'GSCC03': 123., 'GSCC04':120.}

    acres_to_cfs = areas[sitecode]*43560.
    acres_to_sqmiles = areas[sitecode]*0.0015625

    try:
        # total q in inches per acre
        total_q_area_ft = (totalq/acres_to_cfs)
        # fixed -- total q area incheas should be feet * 12 not divided by 12
        total_q_area_inches = total_q_area_ft*12.
    except Exception:
        total_q_area_ft = None
        total_q_area_inches = None

    try:
        # cfs per square mile
        inst_q_area = (instq/acres_to_sqmiles)
    except Exception:
        inst_q_area = None

    try:
        # mean cfs per square mile
        mean_q_area = (meanq/acres_to_sqmiles)
    except Exception:
        mean_q_area = None

    return inst_q_area, total_q_area_inches, mean_q_area

def name_my_csv(sitecode, wateryear, type_of_data):
    """
    smart way to name csvs.
    """

    if type_of_data != "d" and type_of_data !="s":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_high.csv"
    elif type_of_data == "d":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_daily.csv"
    elif type_of_data == "s":
        csvfilename = sitecode.upper() + "_" + str(wateryear) + "_spoints.csv"
    else:
        csvfilename = "TEMP_CSV.csv"
        print "TEMP_CSV.csv used for output! WARNING!"

    return csvfilename

def print_five_minute_file(final_dictionary, sitecode, wateryear, interval_length, original_data, sample_dates):
    """ Creates the five minute values -- now including sample dates!"""

    if sample_dates != None:
        # go from 1 to end of sample dates because we added in the first day to do the first "calculation"
        ordered_samples = iter(sample_dates[1:])
        # test each sample in order rather than all
        given_sample = ordered_samples.next()
    else:
        # give some ridiculous value for given sample so that it will never test "S"
        given_sample = datetime.datetime(0001,1,1,0,0)

    csvfilename = name_my_csv(sitecode, wateryear, interval_length)

    with open(csvfilename,'wb') as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter = ",")

        writer.writerow(['STCODE', 'FORMAT', 'SITECODE', 'WATERYEAR', 'DATE_TIME', 'EQN_SET_CODE', 'STAGE', 'INST_Q', 'INST_Q_AREA', 'INTERVAL', 'MEAN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_INT', 'EST_CODE', 'EVENT_CODE'])

        sorted_dates = sorted(final_dictionary.keys())

        for index, each_date in enumerate(sorted_dates):
            stage = final_dictionary[each_date]['stage']
            instq = final_dictionary[each_date]['inst_q']
            totalq = final_dictionary[each_date]['total_q']
            eqn_set = final_dictionary[each_date]['eqn_set']
            flag  = original_data[each_date]['fval']
            event = original_data[each_date]['event']

            try: 
                # test that a date is not a sample date
                if each_date == given_sample:
                    flag = 'S'
                    given_sample = ordered_samples.next()
                else:
                    pass
            except Exception:
                pass

            # if its not the first value - the mean value computed to the "end" of the interval should be reflected in the previous entry; the total also
            if index != 0:
                meanq = final_dictionary[sorted_dates[index-1]]['mean_q']
                totalq = final_dictionary[sorted_dates[index-1]]['total_q']
            else:
                meanq = final_dictionary[each_date]['mean_q']
                totalq = final_dictionary[each_date]['total_q']

            iqa, tqa, mqa =  to_area(sitecode, instq, totalq, meanq)

            dt = datetime.datetime.strftime(each_date,'%Y-%m-%d %H:%M:%S')
            study_code = "HF004"
            entity = 1

            interval = interval_length

            # sometimes Adam's flag has extra quotes in it. Sometimes it doesn't.
            if "\"M\"" in flag:
                flag = "M"
            else:
                pass

            try:
                new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, round(float(stage),3), round(float(instq),3), round(float(iqa),3), str(interval), round(float(meanq),3), round(float(mqa),3), round(float(tqa),7), flag, event]

            except Exception:
                if stage == None or stage == "None":
                    new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, 'None', 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]
                elif stage != None:
                    try:
                        new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, round(float(stage),3), 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]
                    except Exception:
                        new_row = [study_code, entity, sitecode, wateryear, dt, eqn_set, 'None', 'None', 'None', str(interval), 'None', 'None', 'None', flag, event]

            writer.writerow(new_row)

            # except KeyError:
            #     import pdb; pdb.set_trace();

    print("finished processing the five minute data")

def create_monthly_files(sitecode, wateryear, daily_dictionary):

    md = {}

    csvfilename_m = 'HF004_' + sitecode + "_including_wy" + str(wateryear) + "_monthly.csv"

    stcode = 'HF004'
    format = '2'
    sorted_dates = sorted(daily_dictionary.keys())

    # we already have a way to write the daily csv, so this is just for monthly and annual
    with open(csvfilename_m, 'wb') as writefile_m:
        writer_m = csv.writer(writefile_m, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers_m = ['STCODE', 'FORMAT', 'SITECODE', 'WATERYEAR', 'DATE', 'MEAN_Q', 'MAX_Q', 'MIN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_AREA', 'ESTCODE','ESTDAYS', 'TOTAL_DAYS']

        writer_m.writerow(headers_m)

        for each_day in sorted_dates:
            
            month_found = each_day.month
            year_found = each_day.year

            if each_day >= datetime.datetime(int(wateryear), 10, 1, 0, 0):
                continue
            else:
                pass

            if month_found not in md:
                md[month_found]={'mean':[], 'max':[], 'min': [], 'mqa': [], 'tqa':[], 'flag':[]}
                md[month_found]['mean'].append(daily_dictionary[each_day]['mean'])
                md[month_found]['max'].append(daily_dictionary[each_day]['max'])
                md[month_found]['min'].append(daily_dictionary[each_day]['min'])
                md[month_found]['mqa'].append(daily_dictionary[each_day]['mqa'])
                md[month_found]['tqa'].append(daily_dictionary[each_day]['tqa'])
                md[month_found]['flag'].append(daily_dictionary[each_day]['flag'])
            
            elif month_found in md:
                md[month_found]['mean'].append(daily_dictionary[each_day]['mean'])
                md[month_found]['max'].append(daily_dictionary[each_day]['max'])
                md[month_found]['min'].append(daily_dictionary[each_day]['min'])
                md[month_found]['mqa'].append(daily_dictionary[each_day]['mqa'])
                md[month_found]['tqa'].append(daily_dictionary[each_day]['tqa'])
                md[month_found]['flag'].append(daily_dictionary[each_day]['flag'])

        for each_month in md.keys():
            num_est = len([x for x in md[each_month]['flag'] if x == 'E' or x == 'Q'])
            num_missing = len([x for x in md[each_month]['flag'] if x == 'M'])
            num_tot = len([x for x in md[each_month]['mean']])

            if num_missing/num_tot >= 0.2:
                monthly_flag = "M"
            elif num_missing + num_est >= 0.05 and num_missing < 0.2:
                monthly_flag = "Q"
            elif num_est >= 0.05:
                monthly_flag = "E"
            else: 
                monthly_flag = "A"

            
            if str(each_month) == "10" or str(each_month) == "11" or str(each_month) == "12":
                this_year = str(int(wateryear) -1)
            else:
                this_year = str(wateryear)

            month_mean = str(round(np.mean([float(x) for x in md[each_month]['mean'] if x != "None"]),4))
            month_max = str(round(np.max([float(x) for x in md[each_month]['max'] if x != "None"]),4))
            month_min = str(round(np.min([float(x) for x in md[each_month]['min'] if x != "None"]),4))
            month_mqa = str(round(np.mean([float(x) for x in md[each_month]['mqa'] if x != "None"]),4))
            month_tqa = str(round(sum([float(x) for x in md[each_month]['tqa'] if x != "None"]),4))

            writer_m.writerow([stcode, format, sitecode, wateryear, str(this_year), str(each_month), month_mean, month_max, month_min, month_mqa, month_tqa, monthly_flag, str(num_est), str(num_tot)])
            

def compute_daily_dictionary(sitecode, wateryear, final_dictionary, original_dictionary):
    """
    computes daily as a dictionary annual values
    """
    daily_d = {}
    output_d = {}

    for each_date in sorted(final_dictionary.keys()):

        alt_date = datetime.datetime(each_date.year, each_date.month, each_date.day)

        if alt_date not in daily_d:

            daily_d[alt_date] = {'means':[final_dictionary[each_date]['mean_q']], 'insts':[final_dictionary[each_date]['inst_q']], 'tots':[final_dictionary[each_date]['total_q']], 'flags':[original_dictionary[each_date]['fval']]}

        elif alt_date in daily_d:

            daily_d[alt_date]['means'].append(final_dictionary[each_date]['mean_q'])
            daily_d[alt_date]['insts'].append(final_dictionary[each_date]['inst_q'])
            daily_d[alt_date]['tots'].append(final_dictionary[each_date]['total_q'])
            daily_d[alt_date]['flags'].append(original_dictionary[each_date]['fval'])


    for each_alternate_date in sorted(daily_d.keys()):


        percent_m = len([x for x in daily_d[each_alternate_date]['flags'] if x == "M"])/len(daily_d[each_alternate_date])
        percent_e = len([x for x in daily_d[each_alternate_date]['flags']  if x == "E"])/len(daily_d[each_alternate_date])
        percent_q = len([x for x in daily_d[each_alternate_date]['flags']  if x == "Q"])/len(daily_d[each_alternate_date])

        if percent_m > 0.2:
            daily_flag = "M"
            #daily_d[each_alternate_date].append(daily_flag)
        elif percent_e > 0.05:
            daily_flag = "E"
        elif percent_q > 0.05:
            daily_flag = "Q"
        elif percent_m + percent_e + percent_q > 0.05:
            daily_flag = "Q"
        else:
            daily_flag = "A"


        try:
            _, tqa, mqa = to_area(sitecode, None, sum(daily_d[each_alternate_date]['tots']), np.mean(daily_d[each_alternate_date]['means']))

        except Exception:
            # find the total number of values which are not Null      
            mean_from_not_none_tot = np.mean([x for x in daily_d[each_alternate_date]['means'] if x !=None])
            not_none_tot = sum([x for x in daily_d[each_alternate_date]['tots'] if x != None])
            _, tqa, mqa = to_area(sitecode, None, not_none_tot, mean_from_not_none_tot)

        try:
            # same format as the csv for daily but to a dictionary
            if each_alternate_date not in output_d:
                output_d[each_alternate_date] = {'mean': str(round(np.mean(daily_d[each_alternate_date]['means']),4)), 'max': str(round(np.max(daily_d[each_alternate_date]['insts']),4)), 'min': str(round(np.min(daily_d[each_alternate_date]['insts']),4)), 'mqa': str(round(mqa),4), 'tqa': str(round(tqa),4), 'flag': daily_flag }
            elif each_alternate_date in output_d:
                print("the alternate date is already listed?")
            
        except Exception:

            not_none_mean_day = [x for x in daily_d[each_alternate_date]['means'] if x != None]
            not_none_inst_day = [x for x in daily_d[each_alternate_date]['insts'] if x != None]

            try:

                if each_alternate_date not in output_d: 
                    output_d[each_alternate_date] = {'mean': str(round(np.mean(not_none_mean_day),4)), 'max': str(round(np.max(not_none_inst_day),4)), 'min': str(round(np.min(not_none_inst_day),4)), 'mqa': str(round(mqa,4)), 'tqa': str(round(tqa,4)), 'flag': daily_flag}
                elif each_alternate_date in output_d:
                    print("the alternate date is already listed- part 2 error with Nones")

            except Exception:
                if each_alternate_date not in output_d: 
                    output_d[each_alternate_date] = {'mean': str(round(np.mean(not_none_mean_day),4)), 'max': str(round(np.max(not_none_inst_day),4)), 'min': str(round(np.min(not_none_inst_day),4)), 'mqa': "None", 'tqa': "None", 'flag': daily_flag}
                elif each_alternate_date in output_d:
                    print("the alternate date is already listed- part 2 error with Nones")


    return output_d

def print_daily_values(sitecode, wateryear, final_dictionary, original_dictionary):
    """
    creates a daily output csv
    """
    csvfilename = name_my_csv(sitecode, wateryear, "d")

    daily_d = {}

    stcode = 'HF004'
    format = '2'
    sorted_dates = sorted(final_dictionary.keys())


    with open(csvfilename, 'wb') as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers = ['STCODE', 'FORMAT', 'SITECODE', 'WATERYEAR', 'DATE', 'MEAN_Q', 'MAX_Q', 'MIN_Q', 'MEAN_Q_AREA', 'TOTAL_Q_AREA', 'ESTCODE']

        writer.writerow(headers)

        for each_date in sorted(final_dictionary.keys()):

            alt_date = datetime.datetime(each_date.year, each_date.month, each_date.day)

            if alt_date not in daily_d:

                daily_d[alt_date] = {'means':[final_dictionary[each_date]['mean_q']], 'insts':[final_dictionary[each_date]['inst_q']], 'tots':[final_dictionary[each_date]['total_q']], 'flags':[original_dictionary[each_date]['fval']]}

            elif alt_date in daily_d:

                daily_d[alt_date]['means'].append(final_dictionary[each_date]['mean_q'])
                daily_d[alt_date]['insts'].append(final_dictionary[each_date]['inst_q'])
                daily_d[alt_date]['tots'].append(final_dictionary[each_date]['total_q'])
                daily_d[alt_date]['flags'].append(original_dictionary[each_date]['fval'])


        for each_alternate_date in sorted(daily_d.keys()):

            percent_m = len([x for x in daily_d[each_alternate_date]['flags'] if x == "M"])/len(daily_d[each_alternate_date])
            percent_e = len([x for x in daily_d[each_alternate_date]['flags']  if x == "E"])/len(daily_d[each_alternate_date])
            percent_q = len([x for x in daily_d[each_alternate_date]['flags']  if x == "Q"])/len(daily_d[each_alternate_date])

            if percent_m > 0.2:
                daily_flag = "M"
                #daily_d[each_alternate_date].append(daily_flag)
            elif percent_e > 0.05:
                daily_flag = "E"
            elif percent_q > 0.05:
                daily_flag = "Q"
            elif percent_m + percent_e + percent_q > 0.05:
                daily_flag = "Q"
            else:
                daily_flag = "A"


            try:
                _, tqa, mqa = to_area(sitecode, None, sum(daily_d[each_alternate_date]['tots']), np.mean(daily_d[each_alternate_date]['means']))

            except Exception:
                not_none_tot = sum([x for x in daily_d[each_alternate_date]['tots'] if x != None])
                mean_from_not_none_tot = np.mean([x for x in daily_d[each_alternate_date]['means'] if x !=None])
                _, tqa, mqa = to_area(sitecode, None, not_none_tot, mean_from_not_none_tot)

            try:
                new_row = [stcode, format, sitecode, wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(np.mean(daily_d[each_alternate_date]['means']),4)), str(round(np.max(daily_d[each_alternate_date]['insts']),4)), str(round(np.min(daily_d[each_alternate_date]['insts']),4)), str(round(mqa,4)), str(round(tqa,4)), daily_flag]

            except Exception:

                not_none_mean_day = [x for x in daily_d[each_alternate_date]['means'] if x != None]
                not_none_inst_day = [x for x in daily_d[each_alternate_date]['insts'] if x != None]

                try:
                    new_row = [stcode, format, sitecode , wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(np.mean(not_none_mean_day),4)), str(round(np.max(not_none_inst_day),4)), str(round(np.min(not_none_inst_day),4)), str(round(mqa,4)), str(round(tqa,4)), daily_flag]

                except Exception:
                    new_row = [stcode, format, sitecode , wateryear, datetime.datetime.strftime(each_alternate_date, '%Y-%m-%d'), str(round(np.mean(not_none_mean_day),4)), str(round(np.max(not_none_inst_day),4)), str(round(np.min(not_none_inst_day),4)), "None", "None", daily_flag]

            writer.writerow(new_row)

def print_sDate_values(csvfilename, final_dictionary, sitecode_in, sDate_list):
    """ prints the sDates and total q area between them if if it possible"""

    sDate_d = {}
    areas = {'GSWS01': 237., 'GSWS02': 149., 'GSWS03': 250., 'GSWS06':32, 'GSWS07':38., 'GSWS08':53., 'GSWS09':21., 'GSWS10':25.3, 'GSWSMA':1436., 'GSWSMF':1436., 'GSCC01':171., 'GSCC02': 169., 'GSCC03': 123., 'GSCC04':120.}

    stcode = 'HF004'
    format = '1'
    sitecode = sitecode_in
    sorted_dates = sorted(final_dictionary.keys())

    wateryear = sorted_dates[0].year + 1

    with open(csvfilename, 'wb') as writefile:
        writer = csv.writer(writefile, quoting = csv.QUOTE_NONNUMERIC, delimiter=",")

        headers = ['STCODE', 'FORMAT' ,'SITECODE', 'WATERYEAR', 'BEGIN_DATETIME', 'END_DATETIME', 'TOTAL_Q_SMPL','ESTCODE']

        writer.writerow(headers)

        # add an extra copy of the final day to act as a buffer for the second index
        sDate_list.append(datetime.datetime(int(wateryear),10,1,0,0))

        starting = iter(sDate_list)
        this_date = starting.next()
        subsequent = starting.next()

        # these are the final outputs from the data
        sorted_dates = sorted(final_dictionary.keys())

        for each_date in sorted_dates:

            try:

                if type(this_date) != datetime.datetime:
                    this_date = datetime.datetime.strptime(this_date, '%Y-%m-%d %H:%M:%S')
                    print "converted this date to correct format"

                if type(each_date) != datetime.datetime:
                    each_date = datetime.datetime.strptime(each_date, '%Y-%m-%d %H:%M:%S')
                    print "converted each date to correct format"

                if each_date>= this_date and each_date<subsequent:

                    if this_date not in sDate_d:
                        sDate_d[this_date] = {'total_q':[final_dictionary[each_date]['total_q']] }
                    elif this_date in sDate_d:
                        if final_dictionary[each_date]['total_q'] != None:
                            sDate_d[this_date]['total_q'].append(final_dictionary[each_date]['total_q'])
                        else:
                            pass

                elif each_date == subsequent:

                    this_date = subsequent
                    subsequent = starting.next()   

                    if this_date not in sDate_d:
                        sDate_d[this_date] = {'total_q':[final_dictionary[each_date]['total_q']] }
                    elif this_date in sDate_d:
                        if final_dictionary[each_date]['total_q'] !=None:
                            sDate_d[this_date]['total_q'].append(final_dictionary[each_date]['total_q'])
                        else:
                            pass

                elif each_date > subsequent:
                    pass

                    # if you are at the end of the data you can comment this in to see what dates still exist
                    # print "found: " + datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S') + " which is bigger than the last day"
            
            except Exception:
                pass


        for index,each_date in enumerate(sorted(sDate_d.keys())):

            # create a date that you can print based on the first date
            print_date = datetime.datetime.strftime(each_date, '%Y-%m-%d %H:%M:%S')
            try: 
                print_date_2 = datetime.datetime.strftime(sorted(sDate_d.keys())[index + 1], '%Y-%m-%d %H:%M:%S')
            except Exception:
                print("S-points have been output to the final available date.")
                return True

            sDate_d[each_date].update({'sample_total': sum(sDate_d[each_date]['total_q'])*12/(43560*areas[sitecode])})

            new_row = [stcode, format, sitecode, wateryear, print_date, print_date_2, round(sDate_d[each_date]['sample_total'],3)]

            writer.writerow(new_row)

if __name__ == "__main__":

    sitecode = sys.argv[1]
    wateryear = sys.argv[2]
    filetype = sys.argv[3]

    print("Now processing FLOW for your sitecode and wateryear. Inputs were : " + str(sitecode) + " and " + str(wateryear) + " and the source/check was " + str(filetype))

    if filetype.lower() == "csv":

        csvfilename = os.path.join(sitecode.upper() + "_" + str(wateryear) + "_working", sitecode.upper() + "_" + str(wateryear) + "_re.csv")
        print(" getting data from csv file :" + csvfilename)
        o2 = get_data_from_csv(csvfilename)

    elif filetype.lower() == "sql":
        print(" getting data from sql ")
        o2 = get_data_from_sql(cur, sitecode, first_day, last_day)

    elif filetype.lower() == "badcsv":
        o2 = get_data_from_csv(csvfilename)
        quickly_recheck_data(o2)

    else:
        print(" I have no idea where you want to get the data from, try csv or sql ")

    conn, cur = fc()

    o = get_equation_sets(cur, sitecode, wateryear)
    o1 = get_equations_by_value(cur, sitecode, o)
    sd = get_samples_dates(cur, sitecode, wateryear)
    o3 = set_up_iterators(o2, o1)
    o4 = loop_over_data(o3, o1)

    print("printing the five minute file to csv")
    print_five_minute_file(o4, sitecode, wateryear, 5, o2, sd)
    print("printing the daily file to csv")
    print_daily_values(sitecode, wateryear, o4, o2)

    output_5 = sitecode.upper() + "_" + str(wateryear) + "_high.csv"
    output_daily = sitecode.upper() + "_" + str(wateryear) + "_daily.csv"
    output_streamchem = sitecode.upper() + "_" + str(wateryear)+"_spoint.csv"
    #flag_daily_streams(output_5, output_daily)

    if sd != None:  
        print("printing the S codes to csv")
        print_sDate_values(output_streamchem, o4, sitecode, sd)
    else:
       pass

    print("printing the monthly file to csv")
    o_daily = compute_daily_dictionary(sitecode, wateryear, o4, o2)
    create_monthly_files(sitecode, wateryear, o_daily)

    print "Finished creating the pyFLOW"
