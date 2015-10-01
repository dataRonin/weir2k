*****
weir2K - new version
*****

weir2K
########

The last weir2k did not do exactly what was needed. A lot of the problem was that it was based on a version originally implemented in MATLAB in 2013. That version had the standard problems with dealing with vectorized date stamps. Another problem was that the corrections were being applied to the adjusted data. While the program functioned in such a way that because the adjusted data had reached a stable point on the correction boundaries this wasn't an issue, it was confusing for me to explain this and ultimately a future person would not intuit it. To be safe we wanted a version that would instead operate on a special column of data made specifically for doing adjustments.

Thus I spent a weekend writing the new weir2K.

**UPDATES 10-01-2015**

Don discovered that the adjustment is resolving before it should in weir2k. This is because the previous method of adjustment used ratios, and the ratios were causing the solution to be found too early. After testing ratios and differences, I am aware of how the solution should be computed, and have modified it.

Say we have a beginning HG of .214 and a beginning CR of .211. We have an end HG of .04 and an end CR of 0.06. That means at the beginning our adjustment is CR + 0.03 and at the end we have adjustment of CR - 0.02. Thus, 50% of the way into the measurement, we would have an adjustment of CR + 0.005, and 80% of the way in, a measurement of CR - 0.01 etc., should the adjustment be distributed linearly. We can break this down as:

We define the % of the way into the measurement as the Number of Minutes Into the Interval, for example, if we are 7000 minutes into a 10000 minute interval, we are 70% of the way in. The % of way in weights the ending ratio, whereas the Number of Minutes Left in the Interval (in the same case above, 3000 minutes or 30%) weights the beginning.


`(HG at Beginning - CR at Beginning) * (Number of Minutes Left In Interval/ Total Number of Minutes in Interval) + (HG at End - CR at End) * (Number of Minutes Into Interval/ Total Number of Minutes in Interval) + CR at Given Time `


=============
Getting started
=============

* put raw data in the `raw_data` folder
   * raw data names must include the sitecode in caps and the wateryear. 
   * raw data must be csv with the date in either the 0th or 1st column and the c.r. value in the 1st or 2nd column. The 0 - sitecode, 1 - date_time, 2 - value organizations is best.
* put corr_tables in the `corr_table` folder
    * corr_tables must be named with `corr'
* run "first" analysis which will create images and directories. in the `working` directory you will get a file named with the ending of `re
* look at graphs and make changes to the 3th column --> 0 is sitecode, 1 is datetime, 2 is raw data, 3 is your working data, 4 is adjusted, 5 is flag, 6 is event code.
* run data again, this time using the `re` ending

On **the command line**, `weir2k` can run with a simple syntax. To run data for the first time:

.. code-block:: bash

    $ python weir2k.py "GSWS01" "2014" "first"


To run data for the first time, but not to estimate values when they are missing:

.. code-block:: bash

    $ python weir2k.py "GSWS01" "2014" "sparse"


To rerun data:

.. code-block:: bash

    $ python weir2k.py "GSWS01" "2014" "re"


This data should be more persistent versus problems.

=======
To do or report problems
=======

We consider this to be the `working version` of weir2k. To report problems, contact Fox.

pyflow
########

This version of pyflow is identical to the previous version.

You can see its documentation [here](https://github.com/dataRonin/flow2).

We will leave up the repository of old pyflow in case it is informative.

