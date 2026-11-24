FROM python:3.11-slim-bookworm

# Spark tourne sur la JVM : sans Java, PySpark ne démarre pas.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dépendances d'abord : cette couche reste en cache tant que requirements.txt ne change pas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Jars Delta : on démarre une session Spark vide pendant le build pour que Maven
# les télécharge UNE fois et qu'ils soient dans l'image (marche ensuite sans internet).
RUN python -c "from pyspark.sql import SparkSession; from delta import configure_spark_with_delta_pip; configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

# Le code ensuite : il change souvent, on le met en dernier.
COPY . .

CMD ["python", "-c", "import pyspark; print('pyspark', pyspark.__version__)"]