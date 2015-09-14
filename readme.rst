*****
weir2K - new version
*****

weir2K
########

The last weir2k did not do exactly what was needed. A lot of the problem was that it was based on a version originally implemented in MATLAB in 2013. That version had the standard problems with dealing with vectorized date stamps. Another problem was that the corrections were being applied to the adjusted data. While the program functioned in such a way that because the adjusted data had reached a stable point on the correction boundaries this wasn't an issue, it was confusing for me to explain this and ultimately a future person would not intuit it. To be safe we wanted a version that would instead operate on a special column of data made specifically for doing adjustments.

Thus I spent a weekend writing the new weir2K.

pyflow
########

