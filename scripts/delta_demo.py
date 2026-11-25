"""Démo Delta sur une copie de la fixture : historique, time travel, restore, OPTIMIZE, VACUUM."""
import shutil
from pathlib import Path

from delta.tables import DeltaTable

from src.spark_jobs.spark_session import build_spark

SRC = Path("tests/fixtures/silver_delta/positions_tracked")
DST = Path("data/demo/positions_tracked")

# Repartir d'une copie propre à chaque lancement
if DST.exists():
    shutil.rmtree(DST)
shutil.copytree(SRC, DST)

spark = build_spark("delta-demo")
spark.sparkContext.setLogLevel("ERROR")
path = str(DST)


def etat(titre):
    n = spark.read.format("delta").load(path).count()
    print(f"\n=== {titre} : {n} lignes ===")


def historique():
    (DeltaTable.forPath(spark, path).history()
        .select("version", "operation", "operationMetrics")
        .show(truncate=False))


etat("Départ (version 0)")

# 1) Une "erreur" : on supprime tout le 6 mai
table = DeltaTable.forPath(spark, path)
table.delete("service_date = '2026-05-06'")
etat("Après DELETE du 6 mai (version 1)")
historique()

# 2) Time travel : on relit la version 0 sans rien restaurer
v0 = spark.read.format("delta").option("versionAsOf", 0).load(path).count()
print(f"\nTime travel : la version 0 contient toujours {v0} lignes")

# 3) On annule l'erreur
DeltaTable.forPath(spark, path).restoreToVersion(0)
etat("Après RESTORE")

# 4) OPTIMIZE : regrouper les petits fichiers (ici il n'y a rien à regrouper)
DeltaTable.forPath(spark, path).optimize().executeCompaction()

# 5) On supprime à nouveau le 6 mai, cette fois SANS restaurer
DeltaTable.forPath(spark, path).delete("service_date = '2026-05-06'")
etat("Après un deuxième DELETE")
print("Time travel avant VACUUM :",
      spark.read.format("delta").option("versionAsOf", 0).load(path).count(), "lignes en version 0")

# 6) VACUUM : on supprime pour de bon les fichiers que la version actuelle n'utilise plus
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
print("\nFichiers .parquet avant VACUUM :", len(list(DST.rglob("*.parquet"))))
DeltaTable.forPath(spark, path).vacuum(0)
print("Fichiers .parquet après VACUUM  :", len(list(DST.rglob("*.parquet"))))

# 7) Maintenant le time travel vers la version 0 doit casser
try:
    n = spark.read.format("delta").option("versionAsOf", 0).load(path).rdd.count()
    print("Time travel version 0 : encore possible,", n, "lignes")
except Exception as e:
    print("Time travel version 0 : IMPOSSIBLE après VACUUM ->", type(e).__name__)

historique()
spark.stop()