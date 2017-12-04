# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-10. Working with Conditionals
# Run following PySpark code lines, line by line in PySpark shell

def mpgFind(numOfCylinders) :
       if(numOfCylinders == 4 ):
             mpg = 22
       elif(numOfCylinders == 6 ):
            mpg = 18
       else :
            mpg = 16
       return mpg

mpgFind(4)
