# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 3
# Recipe 3-11. Working with Loops.
# Run following PySpark code lines, line by line in PySpark shell

#Step 3-11-1. For loop implementation

def sumUsingForLoop(numbers) :
       sumOfEvenNumbers = 0
       for i in numbers :
           if i % 2 ==0 :
               sumOfEvenNumbers = sumOfEvenNumbers +i
           else :
              pass
       return   sumOfEvenNumbers

numbers = range(1,102)
numbers

sumUsingForLoop(numbers)

#Step 3-11-2. While loop implementation

def sumUsingWhileLoop(numbers) :
      i = 1 
      sumOfEvenNumbers = 0
      while i <= 101 :
           if i % 2 ==0 :
                sumOfEvenNumbers = sumOfEvenNumbers +i
           i = i + 1 
      return  sumOfEvenNumbers

sumUsingWhileLoop(numbers)
