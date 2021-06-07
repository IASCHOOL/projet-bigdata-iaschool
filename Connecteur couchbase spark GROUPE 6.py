from couchbase.cluster import Cluster , ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
#from couchbase.cluster import QueryOptions
from collections import defaultdict
import json
import time
import random
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import findspark
findspark.init()
#from couchbase_core.cluster import PasswordAuthenticator
from pyspark.streaming import StreamingContext

#DEFAULT_USER = 'usercouchbase'
#PASSWORD = "Password@1234567"
DEFAULT_USER = 'admin'
PASSWORD = 'password'
#URL = "couchbase://vm0.server-ikpg3gnyakhka.westeurope.cloudapp.azure.com"
URL = "couchbase://127.0.0.1"


cluster = Cluster(URL, ClusterOptions(PasswordAuthenticator(DEFAULT_USER, PASSWORD)))
bucket = cluster.bucket('default')

transaction_id = 0
card_id = 0
card_type_list = ["Amex", "CB", "Visa", "Mastercard"]
amount = 0

timestamp_event = datetime.datetime.now()
i=0
while(i<100):
    i+=1
    if(random.randint(0, 10)>3):
        delay = random.uniform(0, 0.05)
    else:
        delay = random.uniform(0, 3)
    timestamp_event += datetime.timedelta(seconds=delay)
    transaction_id += 1
    card_id = random.randint(0,10000)
    card_type = card_type_list[random.randint(0,3)]
    amount = random.uniform(0, 1000)

     # create json
    transaction = {}
    transaction['transaction_id'] = transaction_id
    transaction['card_id'] = card_id
    transaction['card_type'] = card_type
    transaction['amount'] = amount
    transaction['@timestamp'] = str(timestamp_event)

    #print("Transsaction: "+ str(i)+" ", transaction)

     # upsert json in couchbase bucket
    bucket.upsert("transaction_"+str(transaction_id), transaction)

    time.sleep(delay)

