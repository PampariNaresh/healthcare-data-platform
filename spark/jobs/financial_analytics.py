"""
Financial Analytics Job
Covers: revenue by doctor, specialization, branch, billing payment,
        outstanding payments, monthly trend, treatment cost.
"""

import logging
from pyspark.sql import Window
from pyspark.sql import functions as F
from utils import read_table, write_table, build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def revenue_by_doctor(appointments, treatments, billing, doctors):
    df = (
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
            F.count("b.bill_id")          .alias("total_bills"),
            F.round(F.sum("b.amount"), 2) .alias("total_revenue"),
            F.round(F.avg("b.amount"), 2) .alias("avg_bill_amount"),
            F.round(F.max("b.amount"), 2) .alias("max_bill_amount"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_revenue_by_doctor")


def revenue_by_specialization(appointments, treatments, billing, doctors):
    df = (
        appointments.alias("a")
        .join(treatments.alias("t"), "appointment_id")
        .join(billing.alias("b"),    "treatment_id")
        .join(doctors.alias("d"),    "doctor_id")
        .groupBy("d.specialization")
        .agg(
            F.countDistinct("d.doctor_id")        .alias("doctor_count"),
            F.count("a.appointment_id")           .alias("total_appointments"),
            F.round(F.sum("b.amount"), 2)         .alias("total_revenue"),
            F.round(F.avg("b.amount"), 2)         .alias("avg_revenue_per_appt"),
        )
        .withColumn("avg_revenue_per_doc",
            F.round(F.col("total_revenue") / F.col("doctor_count"), 2))
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_revenue_by_specialization")


def revenue_by_branch(appointments, treatments, billing, doctors):
    df = (
        appointments.alias("a")
        .join(treatments.alias("t"), "appointment_id")
        .join(billing.alias("b"),    "treatment_id")
        .join(doctors.alias("d"),    "doctor_id")
        .groupBy("d.hospital_branch")
        .agg(
            F.countDistinct("d.doctor_id")   .alias("doctor_count"),
            F.count("a.appointment_id")      .alias("total_appointments"),
            F.round(F.sum("b.amount"), 2)    .alias("total_revenue"),
            F.round(F.avg("b.amount"), 2)    .alias("avg_revenue_per_appt"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_revenue_by_branch")


def billing_payment_summary(billing):
    total_revenue = billing.agg(F.sum("amount")).collect()[0][0] or 1
    df = (
        billing
        .groupBy("payment_method", "payment_status")
        .agg(
            F.count("bill_id")          .alias("bill_count"),
            F.round(F.sum("amount"), 2) .alias("total_amount"),
            F.round(F.avg("amount"), 2) .alias("avg_amount"),
        )
        .withColumn("pct_of_total_revenue",
            F.round(F.col("total_amount") / total_revenue * 100, 2))
        .orderBy("payment_method", "payment_status")
    )
    write_table(df, "analytics_billing_payment")


def outstanding_payments(billing):
    df = (
        billing
        .filter(F.col("payment_status").isin("Pending", "Failed"))
        .groupBy("payment_status")
        .agg(
            F.count("bill_id")          .alias("bill_count"),
            F.round(F.sum("amount"), 2) .alias("total_outstanding"),
            F.round(F.avg("amount"), 2) .alias("avg_outstanding"),
            F.min("bill_date")          .alias("oldest_bill_date"),
        )
        .orderBy("payment_status")
    )
    write_table(df, "analytics_outstanding_payments")


def monthly_revenue_trend(billing):
    lag_window = Window.orderBy("year", "month")
    df = (
        billing
        .withColumn("year",  F.year("bill_date"))
        .withColumn("month", F.month("bill_date"))
        .groupBy("year", "month")
        .agg(
            F.count("bill_id")          .alias("bill_count"),
            F.round(F.sum("amount"), 2) .alias("total_revenue"),
            F.round(F.avg("amount"), 2) .alias("avg_revenue"),
        )
        .withColumn("prev_revenue", F.lag("total_revenue", 1).over(lag_window))
        .withColumn("mom_growth_pct",
            F.when(
                F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") != 0),
                F.round((F.col("total_revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2)
            ).otherwise(None))
        .drop("prev_revenue")
        .orderBy("year", "month")
    )
    write_table(df, "analytics_monthly_revenue")


def treatment_cost_by_type(treatments):
    df = (
        treatments
        .groupBy("treatment_type")
        .agg(
            F.count("treatment_id")   .alias("treatment_count"),
            F.round(F.avg("cost"), 2) .alias("avg_cost"),
            F.round(F.min("cost"), 2) .alias("min_cost"),
            F.round(F.max("cost"), 2) .alias("max_cost"),
            F.round(F.sum("cost"), 2) .alias("total_cost"),
        )
        .orderBy(F.desc("total_cost"))
    )
    write_table(df, "analytics_treatment_cost")


def main():
    spark = build_spark_session("HealthcareFinancialAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Financial Analytics ===")
    appointments = read_table(spark, "appointments").cache()
    treatments   = read_table(spark, "treatments").cache()
    billing      = read_table(spark, "billing").cache()
    doctors      = read_table(spark, "doctors").cache()

    revenue_by_doctor(appointments, treatments, billing, doctors)
    revenue_by_specialization(appointments, treatments, billing, doctors)
    revenue_by_branch(appointments, treatments, billing, doctors)
    billing_payment_summary(billing)
    outstanding_payments(billing)
    monthly_revenue_trend(billing)
    treatment_cost_by_type(treatments)

    log.info("=== Financial Analytics completed (7 tables) ===")
    spark.stop()


if __name__ == "__main__":
    main()
