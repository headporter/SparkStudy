#!/app/python/bin/python
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("DFTest").master('local[*]').getOrCreate()

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

list1 = [row1, row2, row3, row4, row5]

df = spark.createDataFrame(list1, schema)
df.select(df.job, row_number().over(Window.partitionBy(df.job).orderBy(df.age)).alias('rownum')).show()
df.where(df.age.isin(nums.value)).show()


col = when(df.age % 2 == 0, 'even').otherwise('odd').alias('type')
df.select(df.age, col).show()

df2 = df.union(df)
df2.show()

df2.select(collect_list(df2.name)).show()
df2.select(count(df2.name), countDistinct(df2.name)).show()



rdd = spark.sparkContext.parallelize([Row(arr=[1,2,3,4,5]), Row(arr=[6,7,8,9,10])])
df3 = spark.createDataFrame(rdd)

df3.select(df3.arr).show()
df3.select(df3.arr, array_contains(df3.arr, 5), size(df3.arr)).show()

df3.select(explode(df3.arr)).show()
