#import com.couchbase.spark ## scala
#import com.couchbase.spark._
#import com.couchbase.spark.sql._
#import com.couchbase.spark.streaming._
#import couchbase

from couchbase.cluster import Cluster , ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator

#import org.apache.log4j.Logger
### log4j


##### end

#import org.apache.log4j.Level

##
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *

if __name__=='__main__':
    #spark = SparkSession.builder.config('couchbase://127.0.0.1','8091').getOrCreate()
    conf = SparkConf()
    conf.setMaster("local").setAppName("ProjetBigData")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    DEFAULT_USER = 'admin'
    PASSWORD = 'password'
    #URL = "couchbase://vm0.server-ikpg3gnyakhka.westeurope.cloudapp.azure.com"
    URL = "couchbase://127.0.0.1"
    cluster = Cluster(URL, ClusterOptions(PasswordAuthenticator(DEFAULT_USER, PASSWORD)))
    bucket = cluster.bucket('default')
    
    #log4jLogger = sc._jvm.org.apache.log4j 
    #log = log4jLogger.LogManager.getLogger(__name__) 
    
    #log.warn("Hello World!")
    #print(sc.version)
    ssc = StreamingContext(sc, 1)
    transactions = ssc.socketTextStream(URL, 8091)
    print(transactions.count().count())
    words = transactions.flatMap(lambda line: line.split(","))
    #print(words.count())
    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()
    
    ssc.start()
    ssc.awaitTermination()