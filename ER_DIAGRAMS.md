# ER Diagrams — Healthcare Data Platform

Detailed entity-relationship diagrams for all 20 tables in the `healthcare` MySQL database.
Each diagram is generated from the actual SQL schema files.

- **Operational tables** — `EC21/mysql/init.sql` (5 tables, real FK constraints)
- **Analytics tables** — `EC22/mysql/analytical_schema.sql` (15 tables, no FK constraints — daily Spark snapshots)

---

## 1. Operational Tables — `init.sql`

Five core transactional tables. Foreign keys, cardinalities, and cascade rules match `init.sql` exactly.

> `appointments.appt_status` maps to the column `status` in the database
> (`status` is a reserved word in Mermaid's parser).
> `patients.email` and `doctors.email` carry a UNIQUE constraint (not shown — Mermaid does not support UK).

```mermaid
erDiagram
    patients {
        varchar patient_id    PK
        varchar first_name
        varchar last_name
        char    gender
        date    date_of_birth
        varchar contact_number
        varchar address
        date    registration_date
        varchar insurance_provider
        varchar insurance_number
        varchar email
    }

    doctors {
        varchar doctor_id       PK
        varchar first_name
        varchar last_name
        varchar specialization
        varchar phone_number
        tinyint years_experience
        varchar hospital_branch
        varchar email
    }

    appointments {
        varchar appointment_id  PK
        varchar patient_id      FK
        varchar doctor_id       FK
        date    appointment_date
        time    appointment_time
        varchar reason_for_visit
        varchar appt_status
    }

    treatments {
        varchar treatment_id    PK
        varchar appointment_id  FK
        varchar treatment_type
        varchar description
        decimal cost
        date    treatment_date
    }

    billing {
        varchar bill_id         PK
        varchar patient_id      FK
        varchar treatment_id    FK
        date    bill_date
        decimal amount
        varchar payment_method
        varchar payment_status
    }

    patients     ||--o{ appointments : "books"
    doctors      ||--o{ appointments : "handles"
    appointments ||--o{ treatments   : "leads to"
    patients     ||--o{ billing      : "billed to"
    treatments   ||--o{ billing      : "billed as"
```

| FK Constraint | Column | References | ON DELETE |
|---|---|---|---|
| fk_appt_patient | appointments.patient_id | patients.patient_id | CASCADE |
| fk_appt_doctor | appointments.doctor_id | doctors.doctor_id | RESTRICT |
| fk_treat_appointment | treatments.appointment_id | appointments.appointment_id | CASCADE |
| fk_bill_patient | billing.patient_id | patients.patient_id | CASCADE |
| fk_bill_treatment | billing.treatment_id | treatments.treatment_id | RESTRICT |

---

## 2. Financial Analytics — `analytical_schema.sql` (7 tables)

Pre-aggregated by `EC22/spark/jobs/financial_analytics.py`. No FK constraints between tables.
All tables include `last_updated TIMESTAMP` (refreshed every daily Airflow run).

> Composite PKs: `analytics_billing_payment(payment_method, payment_status)` and
> `analytics_monthly_revenue(year, month)` — rendered with a single PK column here;
> the actual composite key is noted in each entity comment.

```mermaid
erDiagram
    analytics_revenue_by_doctor {
        varchar   doctor_id       PK
        varchar   full_name
        varchar   specialization
        varchar   hospital_branch
        int       total_bills
        decimal   total_revenue
        decimal   avg_bill_amount
        decimal   max_bill_amount
        timestamp last_updated
    }

    analytics_revenue_by_specialization {
        varchar   specialization        PK
        int       doctor_count
        int       total_appointments
        decimal   total_revenue
        decimal   avg_revenue_per_doc
        decimal   avg_revenue_per_appt
        timestamp last_updated
    }

    analytics_revenue_by_branch {
        varchar   hospital_branch       PK
        int       doctor_count
        int       total_appointments
        decimal   total_revenue
        decimal   avg_revenue_per_appt
        timestamp last_updated
    }

    analytics_billing_payment {
        varchar   payment_method        PK
        varchar   payment_status
        int       bill_count
        decimal   total_amount
        decimal   avg_amount
        decimal   pct_of_total_revenue
        timestamp last_updated
    }

    analytics_outstanding_payments {
        varchar   payment_status        PK
        int       bill_count
        decimal   total_outstanding
        decimal   avg_outstanding
        date      oldest_bill_date
        timestamp last_updated
    }

    analytics_monthly_revenue {
        smallint  rev_year              PK
        tinyint   rev_month
        int       bill_count
        decimal   total_revenue
        decimal   avg_revenue
        decimal   mom_growth_pct
        timestamp last_updated
    }

    analytics_treatment_cost {
        varchar   treatment_type        PK
        int       treatment_count
        decimal   avg_cost
        decimal   min_cost
        decimal   max_cost
        decimal   total_cost
        timestamp last_updated
    }
```

---

## 3. Operational Analytics — `analytical_schema.sql` (4 tables)

Pre-aggregated by `EC22/spark/jobs/operational_analytics.py`. No FK constraints between tables.

> Composite PK: `analytics_appointment_status(doctor_id, status)` — rendered with `doctor_id` as PK.
> Column `status` renamed `appt_status` and `count` renamed `status_count` (Mermaid reserved words).

```mermaid
erDiagram
    analytics_appointment_status {
        varchar   doctor_id              PK
        varchar   full_name
        varchar   specialization
        varchar   appt_status
        int       status_count
        decimal   pct_of_total
        timestamp last_updated
    }

    analytics_doctor_workload {
        varchar   doctor_id              PK
        varchar   full_name
        varchar   specialization
        varchar   hospital_branch
        int       total_appointments
        int       completed_appointments
        int       unique_patients
        int       no_show_count
        int       cancellation_count
        decimal   no_show_rate_pct
        decimal   cancellation_rate_pct
        decimal   completion_rate_pct
        timestamp last_updated
    }

    analytics_peak_hours {
        tinyint   hour_of_day            PK
        int       appointment_count
        int       completed_count
        int       no_show_count
        decimal   completion_rate_pct
        timestamp last_updated
    }

    analytics_top_doctors_scorecard {
        varchar   doctor_id              PK
        varchar   full_name
        varchar   specialization
        varchar   hospital_branch
        decimal   total_revenue
        decimal   completion_rate_pct
        int       unique_patients
        int       revenue_rank
        int       completion_rank
        decimal   overall_score
        timestamp last_updated
    }
```

---

## 4. Patient Analytics — `analytical_schema.sql` (4 tables)

Pre-aggregated by `EC22/spark/jobs/patient_analytics.py`. No FK constraints between tables.

> Composite PK: `analytics_new_patient_trend(year, month)` — rendered with `trend_year` as PK.
> `year` and `month` renamed `trend_year` / `trend_month` (Mermaid conflicts with SQL function names).

```mermaid
erDiagram
    analytics_patient_spending {
        varchar   insurance_provider    PK
        int       patient_count
        decimal   avg_age
        int       male_count
        int       female_count
        decimal   avg_spend
        decimal   total_spend
        timestamp last_updated
    }

    analytics_patient_age_groups {
        varchar   age_group             PK
        int       patient_count
        int       total_appointments
        decimal   total_spend
        decimal   avg_spend
        varchar   most_common_reason
        timestamp last_updated
    }

    analytics_patient_retention {
        varchar   visit_segment         PK
        int       patient_count
        decimal   pct_of_patients
        decimal   avg_spend
        decimal   total_revenue
        timestamp last_updated
    }

    analytics_new_patient_trend {
        smallint  trend_year            PK
        tinyint   trend_month
        int       new_patients
        int       male_count
        int       female_count
        timestamp last_updated
    }
```
