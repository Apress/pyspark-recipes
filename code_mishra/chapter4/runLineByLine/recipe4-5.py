# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-5. Calculating summary statistics.
# Run following PySpark code lines, line by line in PySpark shell

#Step 4-5-1. Parallelizing the data.

airVelocityKMPH = [12,13,15,12,11,12,11]
parVelocityKMPH = sc.parallelize(airVelocityKMPH,2)

# Step 4-5-2. Number of data points.

countValue =  parVelocityKMPH.count()

#Step 4-5-3. Summation of air velocities over day.

sumValue =  parVelocityKMPH.sum()

#Step 4-5-4.  Mean air velocity over day.

meanValue =  parVelocityKMPH.mean()

#Step 4-5-5. Variance of air data.

varianceValue = parVelocityKMPH.variance()

#Step 4-5-6.  Sample Variance 

sampleVarianceValue =  parVelocityKMPH.sampleVariance()

#Step 4-5-7.  Standard Deviation

stdevValue = parVelocityKMPH.stdev()

#Step 4-5-8. Sample Standard Deviation

sampleStdevValue = parVelocityKMPH.sampleStdev()

#Step 4-5-9. Calculating all in one step 
parVelocityKMPH.stats()
parVelocityKMPH.stats().asDict()
parVelocityKMPH.stats().mean()
parVelocityKMPH.stats().stdev()
parVelocityKMPH.stats().count()
parVelocityKMPH.stats().min()
parVelocityKMPH.stats().max()

