# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 7
# Recipe 7-3 : Read Streaming Data From the console using PySpark Streaming
# Run following PySpark code lines, line by line in PySpark shell

#Step 7-3-2 : Defining a function to do the summation of row data.

def stringToNumberSum(data):
       removedSpaceData = data.strip()
       if   removedSpaceData == '' :
            return(None)
       splittedData =  removedSpaceData.split(' ')
       numData =  [float(x) for x in splittedData]
       sumOfData = sum(numData)
       return (sumOfData)

#Step 7-3-3 : Reading data from netcat server and sum calculation.

from pyspark.streaming import StreamingContext
pysparkBookStreamingContext = StreamingContext(sc, 10)
consoleStreamingData = pysparkBookStreamingContext.socketTextStream(
                                         hostname = 'localhost',
                                         port = 55342
                                        )
sumedData = consoleStreamingData.map(stringToNumberSum)
sumedData.pprint()
pysparkBookStreamingContext.start()
pysparkBookStreamingContext.awaitTerminationOrTimeout(30)

