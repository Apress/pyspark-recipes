# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 4 
# Recipe 4-7. Starting PySpark shell on Mesos.
# Run following PySpark code lines, line by line

#Starting Mesos Master 

mesos-master –work_dir=/allPySpark/mesos/workdir

#Starting Mesos Slave

mesos-slave --master=127.0.0.1:5050 --work_dir=/allPySpark/mesos/workdir1 --systemd_runtime_directory=/allPySpark/mesos/systemd

# Starting PySpark shell using Mesos 
/allPySpark/spark/bin/pyspark --master mesos://127.0.0.1:5050 --conf spark.executor.uri=/home/pysparkbook/binaries/spark-2.0.0-bin-hadoop2.6.tgz

