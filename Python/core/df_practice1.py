#!/app/python/bin/python
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    
    #Starting Point: SparkSession
    spark = SparkSession.builder.appName("DFTest").master('local[*]').getOrCreate()
    
    txtDF = spark.read.text("C:\\Users\\hadoop\\scala_workspace\\spark01\\src\\main\\resources\\people.txt")
    
    wordDF = txtDF.select(explode(split(col("value"), ",")).alias("word"))
    wordDF.groupBy("word").count().show()

    
    
    
    