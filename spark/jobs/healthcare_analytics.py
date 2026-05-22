"""
Healthcare Batch Analytics — PySpark
Reads from MySQL operational tables on EC21 server via JDBC,
computes 15 business analytics, writes results back to MySQL analytical tables.

Required env vars:
    MYSQL_HOST      EC21 EC2 server IP / hostname
    MYSQL_PORT      MySQL port (default 3308)
    MYSQL_DATABASE  Database name (default healthcare)
    MYSQL_USER      MySQL user
    MYSQL_PASSWORD  MySQL password
"""

import os
import logging
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── JDBC config ───────────────────────────────────────────────────────────────

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def read_table(spark, table):
    df = spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
    log.info("  Read %-20s → %d rows", table, df.count())
    return df


def write_table(df, table):
    df.write.jdbc(url=JDBC_URL, table=table, mode="overwrite", properties=JDBC_PROPS)
    log.info("  Written → %-45s (%d rows)", table, df.count())


# ── Analytics ─────────────────────────────────────────────────────────────────

def run_revenue_by_doctor(appointments, treatments, billing, doctors):
    """Total revenue, avg and max bill per doctor."""
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


def run_appointment_status_summary(appointments, doctors):
    """Appointment counts and % breakdown by status per doctor."""
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
        .withColumn("pct_of_total", F.round(F.col("count") / F.col("total_per_doctor") * 100, 2))
        .drop("total_per_doctor")
        .orderBy("doctor_id", "status")
    )
    write_table(df, "analytics_appointment_status")


def run_billing_payment_summary(billing):
    """Revenue breakdown by payment method and status, with % of total."""
    total_revenue = billing.agg(F.sum("amount")).collect()[0][0] or 1
    df = (
        billing
        .groupBy("payment_method", "payment_status")
        .agg(
            F.count("bill_id")          .alias("bill_count"),
            F.round(F.sum("amount"), 2) .alias("total_amount"),
            F.round(F.avg("amount"), 2) .alias("avg_amount"),
        )
        .withColumn("pct_of_total_revenue", F.round(F.col("total_amount") / total_revenue * 100, 2))
        .orderBy("payment_method", "payment_status")
    )
    write_table(df, "analytics_billing_payment")


def run_patient_spending_by_insurance(patients, billing):
    """Patient demographics and spend breakdown per insurance provider."""
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


def run_monthly_revenue_trend(billing):
    """Month-over-month revenue trend with growth %."""
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
        .withColumn(
            "mom_growth_pct",
            F.when(
                F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") != 0),
                F.round((F.col("total_revenue") - F.col("prev_revenue")) / F.col("prev_revenue") * 100, 2)
            ).otherwise(None)
        )
        .drop("prev_revenue")
        .orderBy("year", "month")
    )
    write_table(df, "analytics_monthly_revenue")


def run_treatment_cost_by_type(treatments):
    """Cost statistics per treatment type."""
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


def run_revenue_by_specialization(appointments, treatments, billing, doctors):
    """Which specializations generate the most revenue."""
    df = (
        appointments.alias("a")
        .join(treatments.alias("t"), "appointment_id")
        .join(billing.alias("b"),    "treatment_id")
        .join(doctors.alias("d"),    "doctor_id")
        .groupBy("d.specialization")
        .agg(
            F.countDistinct("d.doctor_id")              .alias("doctor_count"),
            F.count("a.appointment_id")                 .alias("total_appointments"),
            F.round(F.sum("b.amount"), 2)               .alias("total_revenue"),
            F.round(F.avg("b.amount"), 2)               .alias("avg_revenue_per_appt"),
        )
        .withColumn(
            "avg_revenue_per_doc",
            F.round(F.col("total_revenue") / F.col("doctor_count"), 2)
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_revenue_by_specialization")


def run_doctor_workload(appointments, doctors):
    """Per doctor: total appointments, completion, no-show, cancellation rates."""
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
            F.round(F.col("no_show_count")    / F.col("total_appointments") * 100, 2))
        .withColumn("cancellation_rate_pct",
            F.round(F.col("cancellation_count") / F.col("total_appointments") * 100, 2))
        .withColumn("completion_rate_pct",
            F.round(F.col("completed_appointments") / F.col("total_appointments") * 100, 2))
        .orderBy(F.desc("total_appointments"))
    )
    write_table(df, "analytics_doctor_workload")


def run_peak_appointment_hours(appointments):
    """Which hours of day have the most appointments and best completion rate."""
    df = (
        appointments
        .withColumn("hour_of_day", F.hour(F.col("appointment_time").cast("timestamp")))
        .groupBy("hour_of_day")
        .agg(
            F.count("appointment_id")                                         .alias("appointment_count"),
            F.sum(F.when(F.col("status") == "Completed",  1).otherwise(0))   .alias("completed_count"),
            F.sum(F.when(F.col("status") == "No-show",    1).otherwise(0))   .alias("no_show_count"),
        )
        .withColumn("completion_rate_pct",
            F.round(F.col("completed_count") / F.col("appointment_count") * 100, 2))
        .orderBy("hour_of_day")
    )
    write_table(df, "analytics_peak_hours")


def run_patient_age_group_analysis(patients, appointments, billing):
    """Revenue and appointment volume broken down by patient age groups."""
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

    # most common reason per age group using window rank
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
        .join(spend.alias("s"),       "patient_id", "left")
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


def run_patient_retention(appointments, billing):
    """Segment patients by visit frequency and compare their revenue contribution."""
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


def run_new_patient_trend(patients):
    """Monthly new patient registrations with gender split."""
    df = (
        patients
        .withColumn("year",  F.year("registration_date"))
        .withColumn("month", F.month("registration_date"))
        .groupBy("year", "month")
        .agg(
            F.count("patient_id")                                          .alias("new_patients"),
            F.sum(F.when(F.col("gender") == "M", 1).otherwise(0))         .alias("male_count"),
            F.sum(F.when(F.col("gender") == "F", 1).otherwise(0))         .alias("female_count"),
        )
        .orderBy("year", "month")
    )
    write_table(df, "analytics_new_patient_trend")


def run_outstanding_payments(billing):
    """Pending and failed billing amounts — money still to be collected."""
    df = (
        billing
        .filter(F.col("payment_status").isin("Pending", "Failed"))
        .groupBy("payment_status")
        .agg(
            F.count("bill_id")            .alias("bill_count"),
            F.round(F.sum("amount"), 2)   .alias("total_outstanding"),
            F.round(F.avg("amount"), 2)   .alias("avg_outstanding"),
            F.min("bill_date")            .alias("oldest_bill_date"),
        )
        .orderBy("payment_status")
    )
    write_table(df, "analytics_outstanding_payments")


def run_revenue_by_branch(appointments, treatments, billing, doctors):
    """Revenue and appointment volume per hospital branch."""
    df = (
        appointments.alias("a")
        .join(treatments.alias("t"), "appointment_id")
        .join(billing.alias("b"),    "treatment_id")
        .join(doctors.alias("d"),    "doctor_id")
        .groupBy("d.hospital_branch")
        .agg(
            F.countDistinct("d.doctor_id")        .alias("doctor_count"),
            F.count("a.appointment_id")           .alias("total_appointments"),
            F.round(F.sum("b.amount"), 2)         .alias("total_revenue"),
            F.round(F.avg("b.amount"), 2)         .alias("avg_revenue_per_appt"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    write_table(df, "analytics_revenue_by_branch")


def run_top_doctors_scorecard(appointments, treatments, billing, doctors):
    """Composite scorecard: revenue rank + completion rank → overall score."""
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
            F.round(F.sum("b.amount"), 2) .alias("total_revenue"),
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
        # overall score: lower rank number = better; invert so higher = better
        .withColumn("overall_score",
            F.round(
                (1 / F.col("revenue_rank") * 60) +
                (1 / F.col("completion_rank") * 40), 4
            )
        )
        .orderBy(F.desc("overall_score"))
    )
    write_table(df, "analytics_top_doctors_scorecard")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = (
        SparkSession.builder
        .appName("HealthcareAnalytics")
        .config("spark.jars", "/opt/spark/jars/mysql-connector-j.jar")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    log.info("=== Healthcare Analytics Job ===")
    log.info("Connecting to MySQL at %s:%s/%s", MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE)

    # ── Load operational tables ───────────────────────────────────────────────
    patients     = read_table(spark, "patients").cache()
    doctors      = read_table(spark, "doctors").cache()
    appointments = read_table(spark, "appointments").cache()
    treatments   = read_table(spark, "treatments").cache()
    billing      = read_table(spark, "billing").cache()

    # ── Run all analytics ─────────────────────────────────────────────────────
    log.info("--- Financial Analytics ---")
    run_revenue_by_doctor(appointments, treatments, billing, doctors)
    run_revenue_by_specialization(appointments, treatments, billing, doctors)
    run_revenue_by_branch(appointments, treatments, billing, doctors)
    run_billing_payment_summary(billing)
    run_outstanding_payments(billing)
    run_monthly_revenue_trend(billing)
    run_treatment_cost_by_type(treatments)

    log.info("--- Operational Analytics ---")
    run_appointment_status_summary(appointments, doctors)
    run_doctor_workload(appointments, doctors)
    run_peak_appointment_hours(appointments)
    run_top_doctors_scorecard(appointments, treatments, billing, doctors)

    log.info("--- Patient Analytics ---")
    run_patient_spending_by_insurance(patients, billing)
    run_patient_age_group_analysis(patients, appointments, billing)
    run_patient_retention(appointments, billing)
    run_new_patient_trend(patients)

    log.info("=== All 15 analytics jobs completed successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()
