from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

people = spark.createDataFrame([
    {"deptId": 1, "age": 40, "name": "Hyukjin Kwon", "gender": "M", "salary": 50},
    {"deptId": 1, "age": 50, "name": "Takuya Ueshin", "gender": "M", "salary": 100},
    {"deptId": 2, "age": 60, "name": "Xinrong Meng", "gender": "F", "salary": 150},
    {"deptId": 3, "age": 20, "name": "Haejoon Lee", "gender": "M", "salary": 200}
])

ege_column = people.age

department = spark.createDataFrame([
    {"id": 1, "name": "PySpark"},
    {"id": 2, "name": "ML"},
    {"id": 3, "name": "Spark SQL"}
])

people.filter(people.age > 30).join(
    department, people.deptId == department.id).groupBy(
    department.name, "gender").agg({"salary": "avg", "age": "max"}).show()









