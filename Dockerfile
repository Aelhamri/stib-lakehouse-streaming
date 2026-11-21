FROM python:3.11-slim-bookworm

# Spark tourne sur la JVM : sans Java, PySpark ne démarre pas.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dépendances d'abord : cette couche reste en cache tant que requirements.txt ne change pas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Le code ensuite : il change souvent, on le met en dernier.
COPY . .

CMD ["python", "-c", "import pyspark; print('pyspark', pyspark.__version__)"]