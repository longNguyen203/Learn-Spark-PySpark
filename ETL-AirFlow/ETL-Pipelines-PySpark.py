import pandas 

# # Import SparkSession
# from pyspark.sql import SparkSession

# # Create SparkSession 
# spark = SparkSession.builder \
#       .master("local[1]") \
#       .appName("SparkByExamples.com") \
#       .getOrCreate() 

# # Create RDD from parallelize    
# dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
# rdd=spark.sparkContext.parallelize(dataList)

# # Create RDD from external Data source
# rdd2 = spark.sparkContext.textFile("/path/test.txt")

# data = [('James','','Smith','1991-04-01','M',3000),
#   ('Michael','Rose','','2000-05-19','M',4000),
#   ('Robert','','Williams','1978-09-05','M',4000),
#   ('Maria','Anne','Jones','1967-12-01','F',4000),
#   ('Jen','Mary','Brown','1980-02-17','F',-1)
# ]

# columns = ["firstname","middlename","lastname","dob","gender","salary"]
# df = spark.createDataFrame(data=data, schema = columns)

# print(df.show(2))


# from pyspark import SparkContext
# from pyspark.sql import SparkSession

# Initialize a SparkContext (if not already created)
# sc = SparkContext(appName="MyApp")

# Create a SparkSession using the SparkContext
# spark = SparkSession(sc)

# Now you can use 'spark' to work with Spark SQL and DataFrames


