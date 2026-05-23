-- ═══════════════════════════════════════════════════════════════
-- OPERATIONAL DASHBOARD — SQL Queries for Looker Studio
-- Database: healthcare | EC21 MySQL (65.0.80.152:3306)
-- ═══════════════════════════════════════════════════════════════


-- ── workload_kpis ─────────────────────────────────────────────────────────────
-- Used for: Total Appointments, Avg Completion Rate, No-Show Rate, Cancellation Rate scorecards
-- Chart type: Scorecard (one metric each)
SELECT
    SUM(total_appointments)       AS total_appointments,
    SUM(completed_appointments)   AS total_completed,
    SUM(unique_patients)          AS total_unique_patients,
    SUM(no_show_count)            AS total_no_shows,
    SUM(cancellation_count)       AS total_cancellations,
    ROUND(AVG(completion_rate_pct), 2)    AS avg_completion_rate,
    ROUND(AVG(no_show_rate_pct), 2)       AS avg_no_show_rate,
    ROUND(AVG(cancellation_rate_pct), 2)  AS avg_cancellation_rate
FROM analytics_doctor_workload;


-- ── top_doctors ───────────────────────────────────────────────────────────────
-- Used for: Top Doctors Scorecard table
-- Chart type: Table | Sort: revenue_rank ASC
SELECT
    doctor_id,
    full_name,
    specialization,
    hospital_branch,
    total_revenue,
    completion_rate_pct,
    unique_patients,
    revenue_rank,
    completion_rank,
    overall_score
FROM analytics_top_doctors_scorecard
ORDER BY revenue_rank ASC;


-- ── doctor_workload ───────────────────────────────────────────────────────────
-- Used for: Doctor Workload bar chart
-- Chart type: Bar Chart | Dimension: full_name | Metrics: total_appointments, completed_appointments, no_show_count
SELECT
    doctor_id,
    full_name,
    specialization,
    hospital_branch,
    total_appointments,
    completed_appointments,
    unique_patients,
    no_show_count,
    cancellation_count,
    no_show_rate_pct,
    cancellation_rate_pct,
    completion_rate_pct
FROM analytics_doctor_workload
ORDER BY total_appointments DESC;


-- ── appt_status ───────────────────────────────────────────────────────────────
-- Used for: Appointment Status by Doctor stacked bar chart
-- Chart type: Stacked Bar | Dimension: full_name | Breakdown: appt_status | Metric: status_count
SELECT
    doctor_id,
    full_name,
    specialization,
    appt_status,
    status_count,
    pct_of_total
FROM analytics_appointment_status
ORDER BY status_count DESC;


-- ── peak_hours ────────────────────────────────────────────────────────────────
-- Used for: Peak Appointment Hours column chart + Completion Rate by Hour line chart
-- Chart type: Column Chart | Dimension: hour_label | Metric: appointment_count
--             Line Chart  | Dimension: hour_label | Metric: completion_rate_pct
SELECT
    hour_of_day,
    CONCAT(LPAD(hour_of_day, 2, '0'), ':00') AS hour_label,
    appointment_count,
    completed_count,
    no_show_count,
    completion_rate_pct
FROM analytics_peak_hours
ORDER BY hour_of_day ASC;
