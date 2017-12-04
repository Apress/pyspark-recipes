# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-6.  Performing Breadth First Search using GraphFrames.
# Run following PySpark code lines, line by line in PySpark shell
# Start PySpark shell using " pyspark --packages graphframes:graphframes:0.4.0-spark2.0-s_2.11"

#Step 8-6-1 : Creating DataFrames of vertices of given graph. 

from pyspark.sql.types import *
from pyspark.sql import Row
verticesDataList = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
verticesSchema = StructType([StructField('id',StringType(),True)])
verticesRDD = sc.parallelize(verticesDataList, 4)
verticesRDDRows = verticesRDD.map(lambda data : Row(data[0]))
verticesDataFrame = sqlContext.createDataFrame(verticesRDDRows, verticesSchema)
verticesDataFrame.show(4)

#Step 8-6-2 : Creating DataFrames of edge of given graph. 

edgeDataList = [('A','C'),('A','B'),('B','A'),('B','C'),('B','G'),('B','F'),('C','A'),    
                                    ('C','B'),('C','F'),('C','D'),('D','C'),('D','F'),('D','E'),('E','D'),  
                                    ('E','F'),('F','B'),('F','C'),('F','D'),('F','E'),('F','G'),('G','B'),
                                    ('G','F')]

edgeRDD = sc.parallelize(edgeDataList, 4)
edgeRDD.take(4)
edgeRDDRows = edgeRDD.map( lambda data : Row(data[0], data[1]))
edgeRDDRows.take(4)
sourceColumn = StructField('src', StringType(),True)
destinationColumn = StructField('dst', StringType(), True)
edgeSchema = StructType([sourceColumn, destinationColumn])
edgeDataFrame = sqlContext.createDataFrame(edgeRDDRows, edgeSchema)

edgeDataFrame.show(5)

#Step 8-6-3 : Creating GraphFrame object. 

import graphframes.graphframe  as gfm
ourGraph = gfm.GraphFrame(verticesDataFrame, edgeDataFrame)
ourGraph.vertices.show(5)
ourGraph.edges.show(5)

#Step 8-6-3 : Running Breath First Search Algorithm.
bfsPath = ourGraph.bfs(fromExpr="id='D'", toExpr = "id='G'")
bfsPath.show() 




