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

    

    #Creating DataFrames
    df = spark.read.json("C:\\Users\\hadoop\\scala_workspace\\spark01\\src\\main\\resources\\people.json")
    df.show()
    
    #Untyped Dataset Operations (aka DataFrame Operations)
    df.printSchema()
    df.select("name").show()
    df.select(df['name'], df['age'] + 1).show()
    df.filter(df['age'] > 21).show()
    df.groupBy("age").count().show()
    
    #Running SQL Queries Programmatically
    df.createOrReplaceTempView("people")
    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
    
    #Global Temporary View
    df.createGlobalTempView("people")
    spark.sql("SELECT * FROM global_temp.people").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    
    #Interoperating with RDDs
    #Inferring the Schema Using Reflection
    sc = spark.sparkContext
    lines = sc.textFile("C:\\Users\\hadoop\\scala_workspace\\spark01\\src\\main\\resources\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

    schemaPeople = spark.createDataFrame(people)
    schemaPeople.show()
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")
    
    teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    print(type(teenagers))
    teenagers.show()
    teenagers.printSchema()
    
    teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
    for name in teenNames:
        print(name)
    
    
    #Programmatically Specifying the Schema
    sc = spark.sparkContext
    lines = sc.textFile("C:\\Users\\hadoop\\scala_workspace\\spark01\\src\\main\\resources\\people.txt")
    parts = lines.map(lambda l: l.split(","))
    people = parts.map(lambda p: (p[0], p[1].strip()))
    
    schemaString = "name age"
    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)
    
    schemaPeople = spark.createDataFrame(people, schema)
    schemaPeople.createOrReplaceTempView("people")
    schemaPeople.printSchema()
    results = spark.sql("SELECT name, age FROM people")
    results.show()
    
    
    
    