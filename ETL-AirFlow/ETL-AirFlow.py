from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime, date
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

"""Create Dataframe"""
df1 = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

df2 = spark.createDataFrame([
    {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
    {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
    {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
    {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
])

ege_column = df2.age

department = spark.createDataFrame([
    {"id": 1, "name": "PySpark"},
    {"id": 2, "name": "ML"},
    {"id": 3, "name": "Spark SQL"}
])
df2.printSchema()
df2.show()
df2.filter((df2.age > 30) & (df2.gender == "M")).join(
    department, df2.deptId == department.id).groupBy(
    department.name, "gender").agg({"salary": "avg", "age": "max"}).show()
    
data = spark.read.csv("D:\Data_Engineer\TuHocDataEngineer\Project1\sales2019_1.csv")
data_new = data.withColumnRenamed("_c0", "OrderID") \
                .withColumnRenamed("_c1", "Product") \
                .withColumnRenamed("_c2", "QuantityOrdered") \
                .withColumnRenamed("_c3", "PriceEach") \
                .withColumnRenamed("_c4", "OrderDate") \
                .withColumnRenamed("_c5", "PurchaseAddress")
                
data_new.show()
data_new = data_new.withColumn("OrderID", col("OrderID").cast("integer"))
data_new.printSchema()







