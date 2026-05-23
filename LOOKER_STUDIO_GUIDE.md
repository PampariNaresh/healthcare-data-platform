# Looker Studio Dashboards — Healthcare Data Platform

## What is Looker Studio?
Free Google BI tool (formerly Google Data Studio) at https://lookerstudio.google.com
No installation needed — runs in the browser. Connects directly to MySQL on EC21.

---

## Step 1 — Open Port 3306 on EC21

Looker Studio connects from Google's servers, so MySQL port must be public.

In AWS Console:
- EC2 → Security Groups → EC21 security group
- Add Inbound Rule: **Type: MySQL/Aurora | Port: 3306 | Source: 0.0.0.0/0**

---

## Step 2 — Connect Looker Studio to MySQL

1. Go to https://lookerstudio.google.com
2. Click **Create → Data Source**
3. Search for **MySQL** connector → Select it
4. Fill in:

| Field    | Value       |
|----------|-------------|
| Host     | 65.0.80.152 |
| Port     | 3306        |
| Database | healthcare  |
| Username | root        |
| Password | root123     |

5. Click **Authenticate** → **Connect**
6. You will see all tables — click **Create Report**

---

## Step 3 — How to Add a Custom SQL Query Chart

For charts that need aggregated data:
1. In your report → **Add a chart**
2. Click **Add data** → **MySQL** → your connection
3. Toggle **Custom Query** ON
4. Paste the SQL from the queries folder
5. Click **Connect** → configure the chart

---

## Dashboard 1 — Financial Dashboard

Create a new report named **Healthcare — Financial Dashboard**.
Add the following charts using the SQL files in `queries/financial_dashboard.sql`.

| # | Chart Title | SQL Query Name | Chart Type |
|---|-------------|----------------|------------|
| 1 | Total Revenue | revenue_kpis | Scorecard |
| 2 | Total Bills | revenue_kpis | Scorecard |
| 3 | Avg Bill Amount | revenue_kpis | Scorecard |
| 4 | Outstanding Amount | outstanding_kpi | Scorecard |
| 5 | Revenue by Doctor (Top 10) | revenue_by_doctor | Bar Chart |
| 6 | Revenue by Specialization | revenue_by_specialization | Pie Chart |
| 7 | Revenue by Hospital Branch | revenue_by_branch | Bar Chart |
| 8 | Monthly Revenue Trend | monthly_revenue | Line Chart |
| 9 | MoM Growth % | monthly_revenue | Line Chart |
| 10 | Payment Method Breakdown | payment_breakdown | Pie Chart |
| 11 | Outstanding by Status | outstanding_by_status | Bar Chart |
| 12 | Treatment Cost Analysis | treatment_cost | Bar Chart |

**Filters to add:**
- Drop-down: `hospital_branch` (filter all charts)
- Drop-down: `specialization` (filter all charts)

---

## Dashboard 2 — Operational Dashboard

Create a new report named **Healthcare — Operational Dashboard**.
Add the following charts using `queries/operational_dashboard.sql`.

| # | Chart Title | SQL Query Name | Chart Type |
|---|-------------|----------------|------------|
| 1 | Total Appointments | workload_kpis | Scorecard |
| 2 | Avg Completion Rate | workload_kpis | Scorecard |
| 3 | Avg No-Show Rate | workload_kpis | Scorecard |
| 4 | Avg Cancellation Rate | workload_kpis | Scorecard |
| 5 | Top Doctors Scorecard | top_doctors | Table |
| 6 | Doctor Workload | doctor_workload | Bar Chart |
| 7 | Appointment Status by Doctor | appt_status | Stacked Bar |
| 8 | Peak Appointment Hours | peak_hours | Column Chart |
| 9 | Completion Rate by Hour | peak_hours | Line Chart |

**Filters to add:**
- Drop-down: `specialization`
- Drop-down: `hospital_branch`

---

## Dashboard 3 — Patient Dashboard

Create a new report named **Healthcare — Patient Dashboard**.
Add the following charts using `queries/patient_dashboard.sql`.

| # | Chart Title | SQL Query Name | Chart Type |
|---|-------------|----------------|------------|
| 1 | Total Patients | patient_kpis | Scorecard |
| 2 | Avg Patient Spend | patient_kpis | Scorecard |
| 3 | Total Patient Revenue | patient_kpis | Scorecard |
| 4 | New Patients This Year | new_patient_kpi | Scorecard |
| 5 | Spending by Insurance Provider | insurance_spending | Bar Chart |
| 6 | Patient Retention Segments | retention | Pie Chart |
| 7 | Spend by Age Group | age_group_spend | Column Chart |
| 8 | Visit Reason by Age Group | age_group_reasons | Table |
| 9 | New Patient Trend (Monthly) | new_patient_trend | Line Chart |
| 10 | Gender Split by Insurance | gender_split | Stacked Bar |

**Filters to add:**
- Drop-down: `insurance_provider`

---

## Tips

- **Scorecard charts:** Set metric field, remove dimension, enable comparison if needed
- **Sharing:** Click Share → anyone with link can view (free)
- **Scheduled email:** Report settings → Schedule email delivery → set frequency + recipient
- **Refresh:** Looker Studio refreshes data automatically every 12 hours or click Refresh now

---

## How to Name Data Sources (Queries) in Looker Studio

Each SQL query is saved as a named **Data Source** in Looker Studio.

### When Creating a New Data Source

1. Go to **Resource → Manage added data sources → Add a data source**
2. Select **MySQL** → your connection
3. Toggle **Custom Query** ON
4. Paste the SQL query
5. At the top of the page click **"Untitled Data Source"** → type your name
6. Click **Connect** → **Add to Report**

### To Rename an Existing Data Source

1. **Resource → Manage added data sources**
2. Find the data source → click the **pencil (Edit) icon**
3. Click the name at the top → rename it
4. Click **Done**

### Recommended Naming Convention

Use the prefix **HC -** so all healthcare data sources are grouped together.

| SQL Query | Data Source Name |
|-----------|-----------------|
| revenue_kpis | HC - Revenue KPIs |
| revenue_by_doctor | HC - Revenue by Doctor |
| revenue_by_specialization | HC - Revenue by Specialization |
| revenue_by_branch | HC - Revenue by Branch |
| monthly_revenue | HC - Monthly Revenue |
| payment_breakdown | HC - Payment Breakdown |
| outstanding_by_status | HC - Outstanding Payments |
| treatment_cost | HC - Treatment Cost |
| workload_kpis | HC - Workload KPIs |
| top_doctors | HC - Top Doctors Scorecard |
| doctor_workload | HC - Doctor Workload |
| appt_status | HC - Appointment Status |
| peak_hours | HC - Peak Hours |
| patient_kpis | HC - Patient KPIs |
| new_patient_kpi | HC - New Patient KPI |
| insurance_spending | HC - Insurance Spending |
| retention | HC - Patient Retention |
| age_group_spend | HC - Age Group Spend |
| age_group_reasons | HC - Age Group Reasons |
| new_patient_trend | HC - New Patient Trend |
| gender_split | HC - Gender Split |
