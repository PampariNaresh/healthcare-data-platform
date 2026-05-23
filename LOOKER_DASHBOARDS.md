# Looker BI Dashboards — Healthcare Data Platform

## Overview

Looker connects directly to the `healthcare` MySQL database on EC21 and reads from the 15
pre-aggregated analytics tables written daily by Spark. Three dashboards cover Financial,
Operational, and Patient analytics — each built from LookML files stored in `EC22/looker/`.

---

## Connection Setup

In Looker → **Admin → Connections → New Connection**:

| Field    | Value            |
|----------|------------------|
| Name     | healthcare-mysql |
| Dialect  | MySQL            |
| Host     | 65.0.80.152      |
| Port     | 3306             |
| Database | healthcare       |
| Username | root             |
| Password | root123          |

> **Note:** Port 3306 must be open in EC21's EC2 security group (inbound TCP 3306).

---

## LookML Project Setup

1. In Looker → **Develop → New LookML Project** → name it `healthcare`
2. Upload all files from `EC22/looker/` maintaining the folder structure:

```
looker/
├── healthcare.model.lkml          ← connection config + 15 explores
├── views/
│   ├── views_financial.lkml       ← 7 financial analytics tables
│   ├── views_operational.lkml     ← 4 operational analytics tables
│   └── views_patient.lkml         ← 4 patient analytics tables
└── dashboards/
    ├── financial_dashboard.dashboard.lookml
    ├── operational_dashboard.dashboard.lookml
    └── patient_dashboard.dashboard.lookml
```

3. In `healthcare.model.lkml` set `connection` to match your Looker connection name
4. Click **Validate LookML** → **Deploy to Production**

---

## Dashboard 1 — Financial Dashboard

**Source tables:** 7 financial analytics tables
**Filters:** Hospital Branch, Specialization

| Tile | Table | Chart Type |
|------|-------|------------|
| Total Revenue (KPI) | analytics_revenue_by_doctor | Single Value |
| Total Bills (KPI) | analytics_revenue_by_doctor | Single Value |
| Avg Bill Amount (KPI) | analytics_revenue_by_doctor | Single Value |
| Outstanding Amount (KPI) | analytics_outstanding_payments | Single Value |
| Revenue by Doctor (Top 10) | analytics_revenue_by_doctor | Horizontal Bar |
| Revenue by Specialization | analytics_revenue_by_specialization | Pie |
| Revenue by Hospital Branch | analytics_revenue_by_branch | Column |
| Monthly Revenue Trend | analytics_monthly_revenue | Line |
| Payment Method Breakdown | analytics_billing_payment | Pie |
| Outstanding Payments by Status | analytics_outstanding_payments | Column |
| Treatment Cost Analysis | analytics_treatment_cost | Horizontal Bar |

---

## Dashboard 2 — Operational Dashboard

**Source tables:** 4 operational analytics tables
**Filters:** Specialization, Hospital Branch

| Tile | Table | Chart Type |
|------|-------|------------|
| Total Appointments (KPI) | analytics_doctor_workload | Single Value |
| Avg Completion Rate (KPI) | analytics_doctor_workload | Single Value |
| Avg No-Show Rate (KPI) | analytics_doctor_workload | Single Value |
| Avg Cancellation Rate (KPI) | analytics_doctor_workload | Single Value |
| Top Doctors Scorecard | analytics_top_doctors_scorecard | Grid Table |
| Doctor Workload — Appointments & Rates | analytics_doctor_workload | Bar |
| Appointment Status by Doctor | analytics_appointment_status | Bar |
| Peak Appointment Hours | analytics_peak_hours | Column |
| Completion Rate by Hour | analytics_peak_hours | Line |

---

## Dashboard 3 — Patient Dashboard

**Source tables:** 4 patient analytics tables
**Filters:** Insurance Provider

| Tile | Table | Chart Type |
|------|-------|------------|
| Total Patients (KPI) | analytics_patient_spending | Single Value |
| Avg Patient Spend (KPI) | analytics_patient_spending | Single Value |
| Total Patient Revenue (KPI) | analytics_patient_spending | Single Value |
| New Patients This Year (KPI) | analytics_new_patient_trend | Single Value |
| Spending by Insurance Provider | analytics_patient_spending | Bar |
| Patient Retention Segments | analytics_patient_retention | Pie |
| Spend by Age Group | analytics_patient_age_groups | Column |
| Most Common Visit Reason by Age Group | analytics_patient_age_groups | Grid Table |
| New Patient Trend (Monthly) | analytics_new_patient_trend | Line |
| Gender Split by Insurance Provider | analytics_patient_spending | Column |

---

## Data Flow

```
MySQL EC21 (healthcare DB)
  └── 15 analytics tables (refreshed daily by Airflow + Spark on EC22)
        └── Looker connects via JDBC (port 3306)
              ├── financial_dashboard   (7 tables)
              ├── operational_dashboard (4 tables)
              └── patient_dashboard     (4 tables)
```

---

## Analytics Tables Reference

### Financial (7 tables)

| Table | Primary Key | Description |
|-------|-------------|-------------|
| analytics_revenue_by_doctor | doctor_id | Revenue, bills, avg per doctor |
| analytics_revenue_by_specialization | specialization | Revenue aggregated by specialty |
| analytics_revenue_by_branch | hospital_branch | Revenue aggregated by branch |
| analytics_billing_payment | payment_method | Breakdown by payment method/status |
| analytics_outstanding_payments | payment_status | Unpaid/pending bill summary |
| analytics_monthly_revenue | (year, month) | Monthly trend with MoM growth % |
| analytics_treatment_cost | treatment_type | Cost stats per treatment type |

### Operational (4 tables)

| Table | Primary Key | Description |
|-------|-------------|-------------|
| analytics_appointment_status | (doctor_id, status) | Status counts per doctor |
| analytics_doctor_workload | doctor_id | Completion, no-show, cancellation rates |
| analytics_peak_hours | hour_of_day | Appointment volume by hour of day |
| analytics_top_doctors_scorecard | doctor_id | Ranked scorecard with overall score |

### Patient (4 tables)

| Table | Primary Key | Description |
|-------|-------------|-------------|
| analytics_patient_spending | insurance_provider | Spend grouped by insurance provider |
| analytics_patient_age_groups | age_group | Visit and spend by age bracket |
| analytics_patient_retention | visit_segment | Retention segments by visit frequency |
| analytics_new_patient_trend | (year, month) | Monthly new patient registrations |

---

## Scheduling Reports

In Looker, open any dashboard → **three-dot menu → Schedule Delivery**:
- **Format:** PDF or CSV
- **Frequency:** Daily / Weekly
- **Recipient:** pamparinaresh2001@gmail.com
