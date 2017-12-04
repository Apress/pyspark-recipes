# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 8
# Recipe 8-7.   Doing Page Rank  using GraphFrames.
# Run following PySpark code lines, line by line in PySpark shell

#Step 8-7-1 : Creating DataFrame of vertices. 
verticesList  = ['a', 'b', 'c', 'd']
verticesListRDD = sc.parallelize(verticesList, 4)
verticesListRowsRDD = verticesListRDD.map( lambda data : Row(data))
verticesListRowsRDD.collect()
verticesSchema = StructType([StructField('id', StringType(), True)])
verticesDataFrame = sqlContext.createDataFrame(verticesListRowsRDD, verticesSchema)
verticesDataFrame.show()
#Step 8-7-2 : Creating DataFrame of edges. 

edgeDataList = [('a','b'), ('a','c'), ('a','d'), ('b', 'c'), 
                                   ('b', 'd'),('c', 'b'), ('d', 'a'), ('d', 'c')]
sourceColumn = StructField('src', StringType(),True)
destinationColumn = StructField('dst', StringType(), True)
edgeSchema = StructType([sourceColumn, destinationColumn])
edgeRDD = sc.parallelize(edgeDataList, 4)
edgeRDD.take(4)
edgeRDDRows = edgeRDD.map( lambda data : Row(data[0], data[1]))
edgeRDDRows.take(4)
edgeDataFrame = sqlContext.createDataFrame(edgeRDDRows, edgeSchema)
edgeDataFrame.show(5)

#Step 8-7-1 : Creating graph. 

import graphframes.graphframe  as gfm
ourGraph = gfm.GraphFrame(verticesDataFrame, edgeDataFrame)
ourGraph.vertices.show(5)

ourGraph.edges.show(5)

#Step 8-7-1 : Running Page Rank Algorithm. 

pageRanks = ourGraph.pageRank(resetProbability=0.15, tol=0.01)
pageRanks
pageRanks.edges.show()
pageRanks.vertices.select('id','pagerank')
pageRanks.vertices.select('id','pagerank').show()

