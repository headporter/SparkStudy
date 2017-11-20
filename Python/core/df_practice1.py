#!/app/python/bin/python
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os

# ===============================================
def printUsage(progName):
    usage ='''
           |[Usage]
           |
           |%s argv1
           |  argv1: Spark master URL to connect to
           '''.replace('           |', '') % progName
    print(usage)

# =============================================== 
if __name__ == "__main__":
    if len(sys.argv) > 0: 
        spark = SparkSession.builder.appName("pyspark_word_count").master(sys.argv[1]).getOrCreate()
    
        txtDF = spark.read.text(os.path.join('../../data', 'README.md'))
    
        wordDF = txtDF.select(explode(split(col("value"), ",")).alias("word"))
        wordDF.groupBy("word").count().show()
        sys.exit(0)
            
    else:
        printUsage(sys.argv[0])
        sys.exit(1)


    
    
    
    