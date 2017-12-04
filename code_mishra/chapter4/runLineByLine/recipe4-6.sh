# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-6. Starting Pyspark shell on StandAlone cluster manager.
# Run following PySpark code lines, line by line

#Step 4-6-1.  Starting StandAlone cluster manager using start-all.sh script.
/allPySpark/spark/sbin/start-all.sh

# Starting PySpark console using StandAlone Master

/allPySpark/spark/bin/pyspark --master spark://localhost.localdomain:7077

# Stopping StanAlone master 
/allPySpark/spark/sbin/stop-all.sh 

#Step 4-6-2.  Starting StandAlone cluster manager using individual script.
/allPySpark/spark/sbin/start-master.sh 
/allPySpark/spark/sbin/start-slave.sh spark://localhost.localdomain:7077

#Stopping Slave 
/allPySpark/spark/sbin/stop-slave.sh 
#Stoping Master
/allPySpark/spark/sbin/stop-master.sh 

