"""
Operational Analytics Job
Covers: appointment status summary, doctor workload,
        peak appointment hours, top doctors scorecard.
"""

import logging
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils import read_table, write_table, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def appointment_status_summary(appointments, doctors):
    window = Window.partitionBy("doctor_id")
    df = (
        appointments.alias("a")
        .join(doctors.alias("d"), "doctor_id")
        .groupBy(
            F.col("d.doctor_id"),
            F.concat_ws(" ", F.col("d.first_name"), F.col("d.last_name")).alias("full_name"),
            F.col("d.specialization"),
            F.col("a.status"),
        )
        .agg(F.count("a.appointment_id").alias("count"))
        .withColumn("total_per_doctor", F.sum("count").over(window))
        .withColumn("pct_of_total",
            F.round(F.col("count") / F.col("total_per_doctor") * 100, 2))
        .drop("total_per_doctor")
        .orderBy("doctor_id", "status")
    )
    write_table(df, "analytics_appointment_status")


def doctor_workload(appointments, doctors):
    df = (
        appointments.alias("a")
        .join(doctors.alias("d"), "doctor_id")
        .groupBy(
            F.col("d.doctor_id"),
            F.concat_ws(" ", F.col("d.first_name"), F.col("d.last_name")).alias("full_name"),
            F.col("d.specialization"),
            F.col("d.hospital_branch"),
        )
        .agg(
            F.count("a.appointment_id")                                           .alias("total_appointments"),
            F.sum(F.when(F.col("a.status") == "Completed",  1).otherwise(0))     .alias("completed_appointments"),
            F.countDistinct("a.patient_id")                                       .alias("unique_patients"),
            F.sum(F.when(F.col("a.status") == "No-show",    1).otherwise(0))     .alias("no_show_count"),
            F.sum(F.when(F.col("a.status") == "Cancelled",  1).otherwise(0))     .alias("cancellation_count"),
        )
        .withColumn("no_show_rate_pct",
            F.round(F.col("no_show_count")          / F.col("total_appointments") * 100, 2))
        .withColumn("cancellation_rate_pct",
            F.round(F.col("cancellation_count")     / F.col("total_appointments") * 100, 2))
        .withColumn("completion_rate_pct",
            F.round(F.col("completed_appointments") / F.col("total_appointments") * 100, 2))
        .orderBy(F.desc("total_appointments"))
    )
    write_table(df, "analytics_doctor_workload")


def peak_appointment_hours(appointments):
    df = (
        appointments
        .withColumn("hour_of_day", F.hour(F.col("appointment_time").cast("timestamp")))
        .groupBy("hour_of_day")
        .agg(
            F.count("appointment_id")                                        .alias("appointment_count"),
            F.sum(F.when(F.col("status") == "Completed", 1).otherwise(0))   .alias("completed_count"),
            F.sum(F.when(F.col("status") == "No-show",   1).otherwise(0))   .alias("no_show_count"),
        )
        .withColumn("completion_rate_pct",
            F.round(F.col("completed_count") / F.col("appointment_count") * 100, 2))
        .orderBy("hour_of_day")
    )
    write_table(df, "analytics_peak_hours")


def top_doctors_scorecard(appointments, treatments, billing, doctors):
    revenue_df = (
        appointments.alias("a")
        .join(treatments.alias("t"), "appointment_id")
        .join(billing.alias("b"),    "treatment_id")
        .join(doctors.alias("d"),    "doctor_id")
        .groupBy(
            F.col("d.doctor_id"),
            F.concat_ws(" ", F.col("d.first_name"), F.col("d.last_name")).alias("full_name"),
            F.col("d.specialization"),
            F.col("d.hospital_branch"),
        )
        .agg(
            F.round(F.sum("b.amount"), 2)  .alias("total_revenue"),
            F.countDistinct("a.patient_id").alias("unique_patients"),
        )
    )

    completion_df = (
        appointments.alias("a")
        .join(doctors.alias("d"), "doctor_id")
        .groupBy("d.doctor_id")
        .agg(
            F.round(
                F.sum(F.when(F.col("a.status") == "Completed", 1).otherwise(0)) /
                F.count("a.appointment_id") * 100, 2
            ).alias("completion_rate_pct")
        )
    )

    rev_window  = Window.orderBy(F.desc("total_revenue"))
    comp_window = Window.orderBy(F.desc("completion_rate_pct"))

    df = (
        revenue_df.alias("r")
        .join(completion_df.alias("c"), "doctor_id")
        .withColumn("revenue_rank",    F.rank().over(rev_window))
        .withColumn("completion_rank", F.rank().over(comp_window))
        .withColumn("overall_score",
            F.round(
                (1 / F.col("revenue_rank") * 60) +
                (1 / F.col("completion_rank") * 40), 4
            ))
        .orderBy(F.desc("overall_score"))
    )
    write_table(df, "analytics_top_doctors_scorecard")


def main():
    spark = build_spark_session("HealthcareOperationalAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Operational Analytics ===")
    appointments = read_table(spark, "appointments").cache()
    doctors      = read_table(spark, "doctors").cache()
    treatments   = read_table(spark, "treatments").cache()
    billing      = read_table(spark, "billing").cache()

    appointment_status_summary(appointments, doctors)
    doctor_workload(appointments, doctors)
    peak_appointment_hours(appointments)
    top_doctors_scorecard(appointments, treatments, billing, doctors)

    log.info("=== Operational Analytics completed (4 tables) ===")
    spark.stop()


if __name__ == "__main__":
    main()
