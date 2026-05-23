-- ═══════════════════════════════════════════════════════════════
-- PATIENT DASHBOARD — SQL Queries for Looker Studio
-- Database: healthcare | EC21 MySQL (65.0.80.152:3306)
-- ═══════════════════════════════════════════════════════════════


-- ── patient_kpis ──────────────────────────────────────────────────────────────
-- Used for: Total Patients, Avg Patient Spend, Total Patient Revenue scorecards
-- Chart type: Scorecard (one metric each)
SELECT
    SUM(patient_count)    AS total_patients,
    ROUND(AVG(avg_spend), 2) AS avg_patient_spend,
    SUM(total_spend)      AS total_patient_revenue,
    ROUND(AVG(avg_age), 1) AS avg_patient_age,
    SUM(male_count)       AS total_male,
    SUM(female_count)     AS total_female
FROM analytics_patient_spending;


-- ── new_patient_kpi ───────────────────────────────────────────────────────────
-- Used for: New Patients This Year scorecard
-- Chart type: Scorecard
SELECT
    SUM(new_patients)   AS total_new_patients,
    SUM(male_count)     AS new_male_patients,
    SUM(female_count)   AS new_female_patients
FROM analytics_new_patient_trend;


-- ── insurance_spending ────────────────────────────────────────────────────────
-- Used for: Spending by Insurance Provider bar chart
-- Chart type: Bar Chart | Dimension: insurance_provider | Metrics: total_spend, patient_count
SELECT
    insurance_provider,
    patient_count,
    avg_age,
    male_count,
    female_count,
    avg_spend,
    total_spend
FROM analytics_patient_spending
ORDER BY total_spend DESC;


-- ── retention ─────────────────────────────────────────────────────────────────
-- Used for: Patient Retention Segments pie chart
-- Chart type: Pie Chart | Dimension: visit_segment | Metric: patient_count
SELECT
    visit_segment,
    patient_count,
    pct_of_patients,
    avg_spend,
    total_revenue
FROM analytics_patient_retention
ORDER BY patient_count DESC;


-- ── age_group_spend ───────────────────────────────────────────────────────────
-- Used for: Spend by Age Group column chart
-- Chart type: Column Chart | Dimension: age_group | Metrics: avg_spend, patient_count
SELECT
    age_group,
    patient_count,
    total_appointments,
    total_spend,
    avg_spend,
    most_common_reason
FROM analytics_patient_age_groups
ORDER BY age_group ASC;


-- ── age_group_reasons ─────────────────────────────────────────────────────────
-- Used for: Visit Reason by Age Group table
-- Chart type: Table | Dimension: age_group
SELECT
    age_group,
    most_common_reason,
    total_appointments,
    patient_count,
    avg_spend,
    total_spend
FROM analytics_patient_age_groups
ORDER BY total_appointments DESC;


-- ── new_patient_trend ─────────────────────────────────────────────────────────
-- Used for: New Patient Trend (Monthly) line chart
-- Chart type: Line Chart | Dimension: year_month | Metrics: new_patients, male_count, female_count
SELECT
    trend_year,
    trend_month,
    CONCAT(trend_year, '-', LPAD(trend_month, 2, '0')) AS year_month,
    new_patients,
    male_count,
    female_count
FROM analytics_new_patient_trend
ORDER BY trend_year ASC, trend_month ASC;


-- ── gender_split ──────────────────────────────────────────────────────────────
-- Used for: Gender Split by Insurance Provider stacked bar chart
-- Chart type: Stacked Bar | Dimension: insurance_provider | Metrics: male_count, female_count
SELECT
    insurance_provider,
    patient_count,
    male_count,
    female_count,
    ROUND(male_count * 100.0 / patient_count, 1)   AS male_pct,
    ROUND(female_count * 100.0 / patient_count, 1) AS female_pct
FROM analytics_patient_spending
ORDER BY patient_count DESC;
