from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if os.name == "nt":
    hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] = hadoop_home + r"\bin;" + os.environ["PATH"]
    
builder = (
    SparkSession.builder.master("local[*]")
    .appName("delta-hello")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

path = "data/scratch/delta_hello"

# Version 0 : on écrit deux lignes
spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"]) \
    .write.format("delta").mode("overwrite").save(path)

# Version 1 : on met à jour la ligne 2 et on ajoute la ligne 3, en une seule opération
updates = spark.createDataFrame([(2, "B"), (3, "c")], ["id", "val"])
(DeltaTable.forPath(spark, path).alias("t")
    .merge(updates.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

print("--- historique ---")
DeltaTable.forPath(spark, path).history().select("version", "operation").show()

print("--- état actuel ---")
spark.read.format("delta").load(path).orderBy("id").show()

print("--- état à la version 0 ---")
spark.read.format("delta").option("versionAsOf", 0).load(path).orderBy("id").show()

spark.stop()