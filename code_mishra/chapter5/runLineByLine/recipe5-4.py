# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 5 
# Recipe 5.4. Page rank algorithm
# Run following PySpark code lines, line by line in PySpark shell

#Step  5-4-1.  Creating nested lists of web pages , its outbound links and rank initialization.

pageLinks =  [['a' ,['b','c','d']],
              ['b', ['d','c']],
              ['c', ['b']],
              ['d', ['a','c']]]
pageRanks =  [['a',1],
              ['b',1],
              ['c',1],
              ['d',1]]
#Step  5-4-2.  Writing a function to calculate contributions.

def rankContribution(uris, rank):
	numberOfUris = len(uris)
	rankContribution = float(rank) / numberOfUris
	newrank =[]
	for uri in uris:
		newrank.append((uri, rankContribution))
	return newrank
#Step  5-4-3.  Creating paired RDDs.

pageLinksRDD  = sc.parallelize(pageLinks, 2)
pageLinksRDD.collect()

pageRanksRDD  = sc.parallelize(pageRanks, 2)
pageRanksRDD.collect()

#Step  5-4-4.  Creating loop for page rank updation.

numIter = 20
s = 0.85


for i in range(numIter):
	linksRank = pageLinksRDD.join(pageRanksRDD)
	contributedRDD = linksRank.flatMap(lambda x : rankContribution(x[1][0],x[1][1]))
	sumRanks = contributedRDD.reduceByKey(lambda v1,v2 : v1+v2)
	pageRanksRDD = sumRanks.map(lambda x : (x[0],(1-s)+s*x[1]))

pageRanksRDD.collect()
