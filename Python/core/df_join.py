#!/app/python/bin/python
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys

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
        spark = SparkSession.builder.appName("DFTest").master(sys.argv[1]).getOrCreate()
        
        nums = spark.sparkContext.broadcast([20,23])
        
        sf1 = StructField('name', StringType(), True)
        sf2 = StructField('age', IntegerType(), True)
        sf3 = StructField('job', StringType(), True)
        
        schema = StructType([sf1, sf2, sf3])
        
        row1 = Row(name='ha1', age=20, job='nothing')
        row2 = Row(name='ha2', age=21, job='student')
        row3 = Row(name='ha3', age=24, job='nothing')
        row4 = Row(name='ha4', age=21, job='student')
        row5 = Row(name='ha5', age=24, job='teacher')
        
        list1 = [row1, row3, row5]
        list2 = [row1, row2, row4]
        
        df = spark.createDataFrame(list1, schema)
        df2 = spark.createDataFrame(list2, schema)
        
        cond = [df.name == df2.name, df.age == df2.age]
        df.join(df2, cond, 'inner').show()
        df.join(df2, df.name == df2.name, 'inner').show()
        sys.exit(0)
            
    else:
        printUsage(sys.argv[0])
        sys.exit(1)
