# Created by Raju Kumar Mishra 
# Book PySpark Recipes
# Chapter 7
# Recipe 7-4 : Integrate PySpark Streaming with Apache Kafka, and read and analyze the data
# Run following PySpark code lines, line by line in PySpark shell

#Step 7-4-1. Starting Zookeeper, creating the topic, starting Apache Kafka broker, starting the console producer. 
#kafka$ bin/zookeeper-server-start.sh config/zookeeper.properties 
#kafka$ bin/kafka-server-start.sh config/server.properties 
#kafka$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pysparkBookTopic
#kafka$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic pysparkBookTopic

#Step 7-4-2. Starting PySpark with spark-streaming-kafka package. 
#$ pyspark --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0

#Step 7-4-3. Creating sum of each row of numbers.  

def stringToNumberSum(data):
       removedSpaceData = data.strip()
       if   removedSpaceData == '' :
            return(None)
       splittedData =  removedSpaceData.split(' ')
       numData =  [float(x) for x in splittedData]
       sumOfData = sum(numData)
       return (sumOfData)

dataInString = '10 10 20 '

stringToNumberSum(dataInString)

#Step 7-4-4. Reading data from Kafka and getting sum of each row. 

from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
bookStreamContext = StreamingContext(sc, 10)
bookKafkaStream = KafkaUtils.createStream( 
								       ssc = bookStreamContext, 
								       zkQuorum = 'localhost:2185', 
								     groupId = 'pysparkBookGroup', 
                                                                               topics = {'pysparkBookTopic':1}
                                                                               )
sumedData = bookKafkaStream.map( lambda data : stringToNumberSum(data[1]))
sumedData.pprint()
bookStreamContext.start()
bookStreamContext.awaitTerminationOrTimeout(30)
