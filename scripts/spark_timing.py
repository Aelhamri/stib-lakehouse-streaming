import time

t0 = time.time()
from src.spark_jobs.spark_session import build_spark

spark = build_spark("timing")
print(f"1. démarrage de la session : {time.time() - t0:.1f}s")

t = time.time()
print(spark.range(1000).count())
print(f"2. petit job JVM seul     : {time.time() - t:.1f}s")

t = time.time()
spark.range(10).rdd.map(lambda x: x).count()
print(f"3. job avec worker Python : {time.time() - t:.1f}s")

t = time.time()
df = spark.createDataFrame([("a", 1)] * 100, ["k", "v"])
df.groupBy("k").count().collect()
print(f"4. groupBy (shuffle)      : {time.time() - t:.1f}s")

spark.stop()
print(f"TOTAL : {time.time() - t0:.1f}s")