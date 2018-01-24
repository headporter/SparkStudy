#!/app/python/bin/python
#-*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
'''
class Row(object):
    def __init__(self, name, age, job):
        self.name = name
        self.age = age
        self.job = job
'''    

sf1 = StructField('name', StringType(), True)
sf2 = StructField('age', IntegerType(), True)
sf3 = StructField('job', StringType(), True)

schema = StructType([sf1, sf2, sf3])

row1 = Row(name='haha', age=20, job='ga g')
row2 = Row(name='hata', age=21, job='ga r')
row3 = Row(name='haya', age=22, job='ga t')
row4 = Row(name='haua', age=23, job='ga y')
row5 = Row(name='haia', age=24, job='ga u')

list1 = [row1, row2, row3, row4, row5]
#.config('spark.sql.warehouse.dir', 'C:\\Users\\hadoop\\scala_workspace\\spark01\\spark-warehouse').enableHiveSupport()
spark = SparkSession.builder.appName('DFFun').master('local[*]').getOrCreate()

df = spark.createDataFrame(list1, schema)
df.select(explode(split(df.job, ' '))).show()
df.createOrReplaceTempView('users')

sql1 = '''
show tables
'''
spark.sql('select * from users where age > 22').show()
spark.sql(sql1).show()

print(df.head())
print(df.collect())
df.describe('age').show()

df.printSchema()
print(df.columns)
print(df.dtypes)
print(df.schema)

rdd = spark.sparkContext.parallelize(list1)
df2 = spark.createDataFrame(rdd)
df2.show()


df.select(df.age + 1).show()
df.select((df.age + 1).alias("age")).show()

