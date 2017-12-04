# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 7
# Recipe 7-1 : Optimize the Page Rank algorithm using PySpark Code
# Run following PySpark code lines, line by line in PySpark shell

pageLinks =  [['a' ,['b','c','d']],
               ['b', ['d','c']],
               ['c', ['b']],
               ['d', ['a','c']]]
pageRanks =  [['a',1],
               ['b',1],
               ['c',1],
               ['d',1]]

numIter = 20

pageRanksRDD  = sc.parallelize(pageRanks, 2).partitionBy(2,hash).persist()
pageLinksRDD  = sc.parallelize(pageLinks, 2).partitionBy(2,hash).persist()
s = 0.85

def rankContribution(uris, rank):
     numberOfUris = len(uris)
     rankContribution = float(rank) / numberOfUris
     newrank =[]
     for uri in uris:
             newrank.append((uri, rankContribution))
     return newrank

for i in range(numIter):
         linksRank = pageLinksRDD.join(pageRanksRDD)
     contributedRDD = linksRank.flatMap(lambda x : rankContribution(x[1][0],x[1][1]))
     sumRanks = contributedRDD.reduceByKey(lambda v1,v2 : v1+v2)
     pageRanksRDD = sumRanks.map(lambda x : (x[0],(1-s)+s*x[1]))
 
pageRanksRDD.collect()

