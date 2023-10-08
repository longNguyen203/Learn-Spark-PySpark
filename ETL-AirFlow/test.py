from pyspark import SparkSession

spark = SparkSession.builder.master("local").appName("Demo").getOrCreate()

data = [
    ("Long", "Nguyen", "2003", 20, "HaNoi"),
    ("Khanh", "Nguyen", "2015", 7, "HaNoi"),
    ("Thanh", "Nguyen", "2008", 15, "HaNoi")
]

columns = ["FirstName", "LastName", "Birth", "Age", "Address"]
df = spark.createDataFrame(data, columns)

print(df.printChema())