FROM apache/airflow:2.9.3

USER root

# ── System deps ───────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# ── Python packages ───────────────────────────────────────────────────────────
RUN pip install --no-cache-dir \
        apache-airflow-providers-apache-spark==4.8.0 \
        pyspark==3.5.0 \
        pymysql==1.1.1 \
        requests==2.31.0

USER root

# ── Stable SPARK_HOME symlink — version-agnostic ──────────────────────────────
RUN PYSPARK_PATH=$(sudo -u airflow python3 -c \
        "import pyspark, os; print(os.path.dirname(pyspark.__file__))") && \
    ln -sfn "$PYSPARK_PATH" /opt/spark_home

ENV SPARK_HOME=/opt/spark_home
ENV PATH="/opt/spark_home/bin:${PATH}"

# ── MySQL JDBC driver (needed by spark-submit running inside this container) ──
RUN mkdir -p /opt/spark/jars && \
    curl -fL https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar \
         -o /opt/spark/jars/mysql-connector-j.jar && \
    chmod 644 /opt/spark/jars/mysql-connector-j.jar

USER airflow

EXPOSE 8080
