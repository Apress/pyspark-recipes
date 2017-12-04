# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe  3-12. Working with NumPy
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-12-3. Creating a two dimensional array using NumPy array() function.

import numpy as NumPy 
temp1 = [15, 16, 17, 17, 18, 17, 16, 14]
temp2 = [14, 15, 17, 17, 16, 17, 16, 15]
temp3 = [16, 15, 17, 18, 17, 16, 15, 14]
temp4 = [16, 17, 18, 19, 17, 15, 15, 14]
temp5 = [16, 15, 17, 17, 17, 16, 15, 13]

dayWiseTemp = NumPy.array([temp1,temp2,temp3,temp4,temp5])
dayWiseTemp

#Step 3-12-4. Creation of a two dimensional array by vertical and column stacking of smaller arrays. 

dayWiseTemp = NumPy.vstack((temp1,temp2,temp3,temp4,temp5))
dayWiseTemp

d6am = NumPy.array([15, 14, 16, 16, 16])
d8am = NumPy.array([16, 15, 15, 17, 15])
d10am = NumPy.array([17, 17, 17, 18, 17])
d12am = NumPy.array([17, 17, 18, 19, 17])
d2pm = NumPy.array([18, 16, 17, 17, 17])
d4pm = NumPy.array([17, 17, 16, 15, 16])
d6pm = NumPy.array([16, 16, 15, 15, 15])
d8pm = NumPy.array([14, 15, 14, 14, 13])

dayWiseTemp = NumPy.column_stack((d6am,d8am,d10am,d12am,d2pm,d4pm,d6pm,d8pm))
dayWiseTemp

#Step 3-12-5. Knowing and changing the data type of array elements. 

dayWiseTemp.dtype
dayWiseTemp = NumPy.array([temp1,temp2,temp3,temp4,temp5],dtype='int32')
dayWiseTemp.dtype
dayWiseTemp = dayWiseTemp.astype('int32')
dayWiseTemp.dtype

#Step 3-12-6.  Knowing shape of a given array.

dayWiseTemp.shape

#Step 3-12-7. Calculate Minimum and Maximum temperature each day.

dayWiseTemp.min(axis=1)
dayWiseTemp.max(axis=1)

#Step 3-12-8. Calculate Minimum and maximum temperature column-wise.

dayWiseTemp.min(axis=0)
dayWiseTemp.max(axis=0)

#Step 3-12-9. Calculate Mean temperature each day and column wise.

dayWiseTemp.mean(axis=1)
dayWiseTemp.mean(axis=0)

#Step 3-12-10. Calculate Standard deviation of temperature each day and column-wise.

dayWiseTemp.std(axis=1)
dayWiseTemp.std(axis=0)

#Step 3-12-11. Calculate Variance of temperature each day and column-wise.

dayWiseTemp.var(axis=1)
dayWiseTemp.var(axis=0)

#Step 3-12-12. Calculate day wise and column-wise median.

NumPy.median(dayWiseTemp,axis=1)
NumPy.median(dayWiseTemp,axis=0)

#Step 3-12-13. Calculate Overall mean of all the gathered temperature data.

NumPy.mean(dayWiseTemp)

#Step 3-12-14.  Calculate Variance and standard deviation over all five days temperature data.

NumPy.var(dayWiseTemp)
NumPy.std(dayWiseTemp)
