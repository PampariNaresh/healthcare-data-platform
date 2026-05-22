"""Shared JDBC helpers for all healthcare analytics jobs."""

import os
import logging

log = logging.getLogger(__name__)

MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3308")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "healthcare")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")

JDBC_URL = (
    f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
)

JDBC_PROPS = {
    "driver":   "com.mysql.cj.jdbc.Driver",
    "user":     MYSQL_USER,
    "password": MYSQL_PASSWORD,
    "truncate": "true",
}

MYSQL_JAR = "/opt/spark/jars/mysql-connector-j.jar"


def read_table(spark, table):
    df = spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
    log.info("  Read %-20s → %d rows", table, df.count())
    return df


def write_table(df, table):
    df.write.jdbc(url=JDBC_URL, table=table, mode="overwrite", properties=JDBC_PROPS)
    log.info("  Written → %s (%d rows)", table, df.count())


def build_spark_session(app_name):
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", MYSQL_JAR)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
