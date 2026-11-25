"""Montre que Delta refuse un schéma différent (schema enforcement) et accepte une évolution explicite."""
import shutil
from pathlib import Path

from pyspark.sql import functions as F

from src.spark_jobs.spark_session import build_spark

SRC = Path("tests/fixtures/silver_delta/positions_tracked")
DST = Path("data/demo/schema_test")

if DST.exists():
    shutil.rmtree(DST)
shutil.copytree(SRC, DST)

spark = build_spark("delta-schema-test")
spark.sparkContext.setLogLevel("ERROR")
path = str(DST)

df = spark.read.format("delta").load(path)
print("Colonnes de la table :", len(df.columns))

# 1) On ajoute une colonne en trop et on essaie d'ajouter (append) dans la table
mauvais = df.limit(10).withColumn("colonne_en_trop", F.lit("x"))
try:
    mauvais.write.format("delta").mode("append").save(path)
    print("1) Append avec colonne en trop : ACCEPTÉ (inattendu)")
except Exception as e:
    print("1) Append avec colonne en trop : REFUSÉ ->", type(e).__name__)
    print("   ", str(e).splitlines()[0][:200])

# 2) Un mauvais type : on change le type d'une colonne (ici on transforme la 1re colonne en texte inutilisable)
n_apres = spark.read.format("delta").load(path).count()
print("   Lignes dans la table après l'échec :", n_apres, "(inchangé = écriture annulée)")

# 3) Évolution de schéma voulue : on l'autorise explicitement avec mergeSchema
mauvais.write.format("delta").mode("append").option("mergeSchema", "true").save(path)
apres = spark.read.format("delta").load(path)
print("3) Avec mergeSchema=true : ACCEPTÉ, colonnes =", len(apres.columns),
      "| lignes =", apres.count())

spark.stop()