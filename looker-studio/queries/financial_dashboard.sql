-- ═══════════════════════════════════════════════════════════════
-- FINANCIAL DASHBOARD — SQL Queries for Looker Studio
-- Database: healthcare | EC21 MySQL (65.0.80.152:3306)
-- Paste each query as a Custom Query data source in Looker Studio
-- ═══════════════════════════════════════════════════════════════


-- ── revenue_kpis ─────────────────────────────────────────────────────────────
-- Used for: Total Revenue, Total Bills, Avg Bill Amount scorecards
-- Chart type: Scorecard (one metric each)
SELECT
    SUM(total_bills)      AS total_bills,
    SUM(total_revenue)    AS total_revenue,
    AVG(avg_bill_amount)  AS avg_bill_amount,
    MAX(max_bill_amount)  AS max_bill_amount
FROM analytics_revenue_by_doctor;


-- ── outstanding_kpi ───────────────────────────────────────────────────────────
-- Used for: Outstanding Amount scorecard
-- Chart type: Scorecard
SELECT
    SUM(total_outstanding) AS total_outstanding,
    SUM(bill_count)        AS outstanding_bills,
    AVG(avg_outstanding)   AS avg_outstanding,
    MIN(oldest_bill_date)  AS oldest_bill_date
FROM analytics_outstanding_payments;


-- ── revenue_by_doctor ─────────────────────────────────────────────────────────
-- Used for: Revenue by Doctor (Top 10) bar chart
-- Chart type: Bar Chart | Dimension: full_name | Metric: total_revenue
-- Sort: total_revenue DESC | Rows: 10
SELECT
    doctor_id,
    full_name,
    specialization,
    hospital_branch,
    total_bills,
    total_revenue,
    avg_bill_amount,
    max_bill_amount
FROM analytics_revenue_by_doctor
ORDER BY total_revenue DESC;


-- ── revenue_by_specialization ─────────────────────────────────────────────────
-- Used for: Revenue by Specialization pie chart
-- Chart type: Pie Chart | Dimension: specialization | Metric: total_revenue
SELECT
    specialization,
    doctor_count,
    total_appointments,
    total_revenue,
    avg_revenue_per_doc,
    avg_revenue_per_appt
FROM analytics_revenue_by_specialization
ORDER BY total_revenue DESC;


-- ── revenue_by_branch ─────────────────────────────────────────────────────────
-- Used for: Revenue by Hospital Branch bar chart
-- Chart type: Bar Chart | Dimension: hospital_branch | Metric: total_revenue
SELECT
    hospital_branch,
    doctor_count,
    total_appointments,
    total_revenue,
    avg_revenue_per_appt
FROM analytics_revenue_by_branch
ORDER BY total_revenue DESC;


-- ── monthly_revenue ───────────────────────────────────────────────────────────
-- Used for: Monthly Revenue Trend line chart + MoM Growth line chart
-- Chart type: Line Chart | Dimension: year_month | Metrics: total_revenue, mom_growth_pct
SELECT
    rev_year,
    rev_month,
    CONCAT(rev_year, '-', LPAD(rev_month, 2, '0')) AS year_month,
    bill_count,
    total_revenue,
    avg_revenue,
    mom_growth_pct
FROM analytics_monthly_revenue
ORDER BY rev_year ASC, rev_month ASC;


-- ── payment_breakdown ─────────────────────────────────────────────────────────
-- Used for: Payment Method Breakdown pie chart
-- Chart type: Pie Chart | Dimension: payment_method | Metric: total_amount
SELECT
    payment_method,
    payment_status,
    bill_count,
    total_amount,
    avg_amount,
    pct_of_total_revenue
FROM analytics_billing_payment
ORDER BY total_amount DESC;


-- ── outstanding_by_status ─────────────────────────────────────────────────────
-- Used for: Outstanding Payments by Status bar chart
-- Chart type: Bar Chart | Dimension: payment_status | Metric: total_outstanding
SELECT
    payment_status,
    bill_count,
    total_outstanding,
    avg_outstanding,
    oldest_bill_date
FROM analytics_outstanding_payments
ORDER BY total_outstanding DESC;


-- ── treatment_cost ────────────────────────────────────────────────────────────
-- Used for: Treatment Cost Analysis bar chart
-- Chart type: Bar Chart | Dimension: treatment_type | Metrics: avg_cost, total_cost
SELECT
    treatment_type,
    treatment_count,
    avg_cost,
    min_cost,
    max_cost,
    total_cost
FROM analytics_treatment_cost
ORDER BY total_cost DESC;
