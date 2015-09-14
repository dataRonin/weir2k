*****
weir2K - new version
*****

weir2K
########

The last weir2k did not do exactly what was needed. A lot of the problem was that it was based on a version originally implemented in MATLAB in 2013. That version had the standard problems with dealing with vectorized date stamps. Another problem was that the corrections were being applied to the adjusted data. While the program functioned in such a way that because the adjusted data had reached a stable point on the correction boundaries this wasn't an issue, it was confusing for me to explain this and ultimately a future person would not intuit it. To be safe we wanted a version that would instead operate on a special column of data made specifically for doing adjustments.

Thus I spent a weekend writing the new weir2K.


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




pyflow
########

