FROM apache/spark:3.5.3-python3 AS spark-base

FROM python:3.11.10-slim-bookworm

WORKDIR /app
ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:/opt/java/openjdk/bin:${PATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
    procps=2:4.0.2-3 \
    && rm -rf /var/lib/apt/lists/*

# Reuse pinned OpenJDK 11 + Apache Spark 3.5.3 from official image (avoids 317MB PyPI tarball)
COPY --from=spark-base /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-base /opt/spark /opt/spark

# Register Spark & Py4J on Python's site-packages regardless of PYTHONPATH overrides
RUN printf "/opt/spark/python\n/opt/spark/python/lib/py4j-0.10.9.7-src.zip\n" \
    > /usr/local/lib/python3.11/site-packages/spark.pth

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --default-timeout=300 --retries=5 -r /app/requirements.txt

# Pre-download Kafka SQL connector and transitive JARs into /opt/spark-jars
RUN python -c "from pyspark.sql import SparkSession; s = SparkSession.builder.master('local[1]').config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3').getOrCreate(); s.stop()" \
    && mkdir -p /opt/spark-jars \
    && cp /root/.ivy2/jars/*.jar /opt/spark-jars/ \
    && rm -rf /root/.ivy2

COPY common /app/common
COPY speed /app/speed

CMD ["python", "-m", "speed.jobs.job_a_console", "--kafka", "kafka:29092", "--checkpoint", "/checkpoints/job_a_console"]
