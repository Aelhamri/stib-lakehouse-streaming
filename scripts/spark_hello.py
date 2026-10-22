from pyspark.sql import SparkSession
import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = ( SparkSession.builder
.master("local[*]")
.appName("Hello")
.getOrCreate() 
)

print("Version de Spark : ", spark.version)

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "txt"])
df.show()

spark.stop()