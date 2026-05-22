
"""
Patient Analytics Job
Covers: patient spending by insurance, age group analysis,
        patient retention, new patient registration trend.
"""

import logging
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils import read_table, write_table, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def patient_spending_by_insurance(patients, billing):
    spend = (
        billing
        .groupBy("patient_id")
        .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
    )
    df = (
        patients.alias("p")
        .withColumn("age", (F.datediff(F.current_date(), F.col("date_of_birth")) / 365).cast("int"))
        .join(spend.alias("s"), "patient_id", "left")
        .groupBy("insurance_provider")
        .agg(
            F.count("p.patient_id")                                .alias("patient_count"),
            F.round(F.avg("age"), 1)                               .alias("avg_age"),
            F.sum(F.when(F.col("gender") == "M", 1).otherwise(0)) .alias("male_count"),
            F.sum(F.when(F.col("gender") == "F", 1).otherwise(0)) .alias("female_count"),
            F.round(F.avg("s.total_spend"), 2)                    .alias("avg_spend"),
            F.round(F.sum("s.total_spend"), 2)                    .alias("total_spend"),
        )
        .orderBy(F.desc("patient_count"))
    )
    write_table(df, "analytics_patient_spending")


def patient_age_group_analysis(patients, appointments, billing):
    age_brackets = (
        patients
        .withColumn("age", (F.datediff(F.current_date(), F.col("date_of_birth")) / 365).cast("int"))
        .withColumn("age_group",
            F.when(F.col("age") <= 18, "0-18")
             .when(F.col("age") <= 35, "19-35")
             .when(F.col("age") <= 50, "36-50")
             .when(F.col("age") <= 65, "51-65")
             .otherwise("65+"))
    )

    spend = (
        billing
        .groupBy("patient_id")
        .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
    )

    appt_counts = (
        appointments
        .groupBy("patient_id")
        .agg(F.count("appointment_id").alias("appt_count"))
    )

    reason_window = Window.partitionBy("age_group").orderBy(F.desc("reason_count"))
    top_reason = (
        age_brackets.alias("ag")
        .join(appointments.alias("a"), "patient_id")
        .groupBy("age_group", "reason_for_visit")
        .agg(F.count("*").alias("reason_count"))
        .withColumn("rank", F.rank().over(reason_window))
        .filter(F.col("rank") == 1)
        .select("age_group", F.col("reason_for_visit").alias("most_common_reason"))
    )

    df = (
        age_brackets.alias("p")
        .join(spend.alias("s"),        "patient_id", "left")
        .join(appt_counts.alias("ac"), "patient_id", "left")
        .groupBy("age_group")
        .agg(
            F.count("p.patient_id")            .alias("patient_count"),
            F.sum("ac.appt_count")             .alias("total_appointments"),
            F.round(F.sum("s.total_spend"), 2) .alias("total_spend"),
            F.round(F.avg("s.total_spend"), 2) .alias("avg_spend"),
        )
        .join(top_reason, "age_group", "left")
        .orderBy("age_group")
    )
    write_table(df, "analytics_patient_age_groups")


def patient_retention(appointments, billing):
    visit_counts = (
        appointments
        .groupBy("patient_id")
        .agg(F.count("appointment_id").alias("visit_count"))
        .withColumn("visit_segment",
            F.when(F.col("visit_count") == 1, "single_visit")
             .when(F.col("visit_count") <= 3, "2-3_visits")
             .when(F.col("visit_count") <= 6, "4-6_visits")
             .otherwise("7+_visits"))
    )

    spend = (
        billing
        .groupBy("patient_id")
        .agg(F.round(F.sum("amount"), 2).alias("total_spend"))
    )

    total_patients = visit_counts.count() or 1

    df = (
        visit_counts.alias("v")
        .join(spend.alias("s"), "patient_id", "left")
        .groupBy("visit_segment")
        .agg(
            F.count("v.patient_id")            .alias("patient_count"),
            F.round(F.avg("s.total_spend"), 2) .alias("avg_spend"),
            F.round(F.sum("s.total_spend"), 2) .alias("total_revenue"),
        )
        .withColumn("pct_of_patients",
            F.round(F.col("patient_count") / total_patients * 100, 2))
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_patient_retention")


def new_patient_trend(patients):
    df = (
        patients
        .withColumn("year",  F.year("registration_date"))
        .withColumn("month", F.month("registration_date"))
        .groupBy("year", "month")
        .agg(
            F.count("patient_id")                                         .alias("new_patients"),
            F.sum(F.when(F.col("gender") == "M", 1).otherwise(0))        .alias("male_count"),
            F.sum(F.when(F.col("gender") == "F", 1).otherwise(0))        .alias("female_count"),
        )
        .orderBy("year", "month")
    )
    write_table(df, "analytics_new_patient_trend")


def main():
    spark = build_spark_session("HealthcarePatientAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Patient Analytics ===")
    patients     = read_table(spark, "patients").cache()
    appointments = read_table(spark, "appointments").cache()
    billing      = read_table(spark, "billing").cache()

    patient_spending_by_insurance(patients, billing)
    patient_age_group_analysis(patients, appointments, billing)
    patient_retention(appointments, billing)
    new_patient_trend(patients)

    log.info("=== Patient Analytics completed (4 tables) ===")
    spark.stop()


if __name__ == "__main__":
    main()
