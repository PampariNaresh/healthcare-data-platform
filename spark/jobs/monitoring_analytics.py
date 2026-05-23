"""
Monitoring Analytics Job
Covers: vitals patient summary, lab test summary, hospital event summary,
        department activity, ICU code summary.
"""

import logging
from pyspark.sql import functions as F
from utils import read_table, write_table, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def vitals_patient_summary(vitals):
    df = (
        vitals
        .groupBy("patient_id")
        .agg(
            F.count("event_id")                       .alias("total_readings"),
            F.sum(F.col("is_anomaly").cast("int"))    .alias("anomaly_count"),
            F.round(F.avg("heart_rate"), 1)           .alias("avg_heart_rate"),
            F.round(F.avg("spo2"), 1)                 .alias("avg_spo2"),
            F.round(F.avg("systolic"), 1)             .alias("avg_systolic"),
            F.round(F.avg("diastolic"), 1)            .alias("avg_diastolic"),
            F.round(F.avg("temperature_celsius"), 2)  .alias("avg_temperature"),
            F.round(F.avg("respiratory_rate"), 1)     .alias("avg_respiratory_rate"),
        )
        .withColumn("anomaly_rate_pct",
            F.round(F.col("anomaly_count") / F.col("total_readings") * 100, 2))
        .orderBy(F.desc("anomaly_rate_pct"))
    )
    write_table(df, "analytics_vitals_patient_summary")


def lab_test_summary(lab_reports):
    total_revenue = lab_reports.agg(F.sum("amount")).collect()[0][0] or 1
    df = (
        lab_reports
        .groupBy("test_name")
        .agg(
            F.count("report_id")                                        .alias("total_tests"),
            F.sum((F.col("flag") == "normal").cast("int"))              .alias("normal_count"),
            F.sum((F.col("flag") == "low").cast("int"))                 .alias("low_count"),
            F.sum((F.col("flag") == "high").cast("int"))                .alias("high_count"),
            F.sum((F.col("flag") == "critical").cast("int"))            .alias("critical_count"),
            F.round(F.avg("amount"), 2)                                 .alias("avg_amount"),
            F.round(F.sum("amount"), 2)                                 .alias("total_revenue"),
        )
        .withColumn("critical_rate_pct",
            F.round(F.col("critical_count") / F.col("total_tests") * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_lab_test_summary")


def hospital_event_summary(hospital_events):
    df = (
        hospital_events
        .groupBy("event_type")
        .agg(
            F.count("event_id")             .alias("event_count"),
            F.round(F.sum("amount"), 2)     .alias("total_amount"),
            F.round(F.avg("amount"), 2)     .alias("avg_amount"),
        )
        .orderBy(F.desc("total_amount"))
    )
    write_table(df, "analytics_hospital_event_summary")


def department_activity(hospital_events, icu_codes, departments):
    events_agg = (
        hospital_events
        .groupBy("department_id")
        .agg(
            F.count("event_id")           .alias("total_events"),
            F.round(F.sum("amount"), 2)   .alias("total_event_amount"),
        )
    )

    icu_agg = (
        icu_codes
        .groupBy("department_id")
        .agg(
            F.count("code_id")                                          .alias("total_icu_codes"),
            F.sum((F.col("severity") == "CRITICAL").cast("int"))        .alias("critical_icu_count"),
            F.round(F.sum("amount"), 2)                                 .alias("total_icu_amount"),
        )
    )

    df = (
        departments.alias("d")
        .join(events_agg.alias("e"), "department_id", "left")
        .join(icu_agg.alias("i"),    "department_id", "left")
        .select(
            F.col("d.department_id"),
            F.col("d.department_name"),
            F.col("d.hospital_branch"),
            F.coalesce(F.col("e.total_events"),      F.lit(0)).alias("total_events"),
            F.coalesce(F.col("i.total_icu_codes"),   F.lit(0)).alias("total_icu_codes"),
            F.coalesce(F.col("i.critical_icu_count"), F.lit(0)).alias("critical_icu_count"),
            F.coalesce(F.col("e.total_event_amount"), F.lit(0.0)).alias("total_event_amount"),
            F.coalesce(F.col("i.total_icu_amount"),   F.lit(0.0)).alias("total_icu_amount"),
            F.round(
                F.coalesce(F.col("e.total_event_amount"), F.lit(0.0)) +
                F.coalesce(F.col("i.total_icu_amount"),   F.lit(0.0)), 2
            ).alias("total_amount"),
        )
        .orderBy(F.desc("total_amount"))
    )
    write_table(df, "analytics_department_activity")


def icu_code_summary(icu_codes):
    df = (
        icu_codes
        .groupBy("code_type", "severity")
        .agg(
            F.count("code_id")            .alias("code_count"),
            F.round(F.sum("amount"), 2)   .alias("total_amount"),
            F.round(F.avg("amount"), 2)   .alias("avg_amount"),
        )
        .orderBy(F.desc("code_count"))
    )
    write_table(df, "analytics_icu_code_summary")


def main():
    spark = build_spark_session("HealthcareMonitoringAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Monitoring Analytics ===")
    vitals          = read_table(spark, "patient_vitals").cache()
    lab_reports     = read_table(spark, "lab_reports").cache()
    hospital_events = read_table(spark, "hospital_events").cache()
    icu_codes       = read_table(spark, "icu_codes").cache()
    departments     = read_table(spark, "departments").cache()

    vitals_patient_summary(vitals)
    lab_test_summary(lab_reports)
    hospital_event_summary(hospital_events)
    department_activity(hospital_events, icu_codes, departments)
    icu_code_summary(icu_codes)

    log.info("=== Monitoring Analytics completed (5 tables) ===")
    spark.stop()


if __name__ == "__main__":
    main()
