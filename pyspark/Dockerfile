FROM apache/spark:3.5.0

USER root

# ── Python packages ───────────────────────────────────────────────────────────
RUN pip3 install --no-cache-dir \
        pyspark==3.5.0 \
        pandas==2.0.3 \
        pyarrow==14.0.2

# ── MySQL JDBC driver ─────────────────────────────────────────────────────────
ADD https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
    /opt/spark/jars/mysql-connector-j.jar
RUN chmod 644 /opt/spark/jars/mysql-connector-j.jar

# ── Spark jobs directory ──────────────────────────────────────────────────────
RUN mkdir -p /opt/spark/jobs && chown spark:spark /opt/spark/jobs

USER spark

WORKDIR /opt/spark/jobs

EXPOSE 8080 7077 8081
