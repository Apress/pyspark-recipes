# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 6 
# Recipe 6-12. Reading table data from HBase using PySpark.
# Start PySpark using pyspark --jars 'spark-examples-1.6.0-hadoop2.6.0.jar','/hbase-client-1.2.4.jar','hbase-common-1.2.4.jar' 
# Run following PySpark code lines, line by line in PySpark shell

>>> hostName = 'localhost'

>>> tableName = 'pysparkBookTable'

>>>   ourInputFormatClass='org.apache.hadoop.hbase.mapreduce.TableInputFormat'
>>> ourKeyClass='org.apache.hadoop.hbase.io.ImmutableBytesWritable'
>>> ourValueClass='org.apache.hadoop.hbase.client.Result'
>>> ourKeyConverter='org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter'
>>> ourValueConverter='org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter'
>>> configuration = {}
>>> configuration['hbase.mapreduce.inputtable'] = tableName
>>> configuration['hbase.zookeeper.quorum'] = hostName

Now it is time to call the function newAPIHadoopRDD() with its arguments. 

>>> tableRDDfromHBase = sc.newAPIHadoopRDD(
...                        inputFormatClass = ourInputFormatClass,
...                        keyClass = ourKeyClass,
...                        valueClass = ourValueClass,
...                        keyConverter = ourKeyConverter,
...                        valueConverter = ourValueConverter,
...                        conf = configuration
...                     )


Let us see how our paired RDD  tableRDDfromHBase looks like. 

>>> tableRDDfromHBase.take(2)

