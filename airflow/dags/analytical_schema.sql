-- ─────────────────────────────────────────────────────────────────────────────
-- Healthcare Analytical Tables — EC22
-- Run against the `healthcare` database on EC21's MySQL server
-- ─────────────────────────────────────────────────────────────────────────────

USE healthcare;

-- ── 1. Revenue by Doctor ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_doctor (
    doctor_id           VARCHAR(10)     NOT NULL,
    full_name           VARCHAR(101)    NOT NULL,
    specialization      VARCHAR(100)    NOT NULL,
    hospital_branch     VARCHAR(100),
    total_bills         INT             NOT NULL DEFAULT 0,
    total_revenue       DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_bill_amount     DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    max_bill_amount     DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (doctor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 2. Appointment Status Summary per Doctor ──────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_appointment_status (
    doctor_id           VARCHAR(10)     NOT NULL,
    full_name           VARCHAR(101)    NOT NULL,
    specialization      VARCHAR(100)    NOT NULL,
    status              VARCHAR(20)     NOT NULL,
    count               INT             NOT NULL DEFAULT 0,
    pct_of_total        DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (doctor_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 3. Billing by Payment Method & Status ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_billing_payment (
    payment_method      VARCHAR(50)     NOT NULL,
    payment_status      VARCHAR(20)     NOT NULL,
    bill_count          INT             NOT NULL DEFAULT 0,
    total_amount        DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_amount          DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    pct_of_total_revenue DECIMAL(5,2)   NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_method, payment_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 4. Patient Spending by Insurance Provider ─────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_spending (
    insurance_provider  VARCHAR(100)    NOT NULL,
    patient_count       INT             NOT NULL DEFAULT 0,
    avg_age             DECIMAL(5,1)    NOT NULL DEFAULT 0.0,
    male_count          INT             NOT NULL DEFAULT 0,
    female_count        INT             NOT NULL DEFAULT 0,
    avg_spend           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    total_spend         DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (insurance_provider)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 5. Monthly Revenue Trend ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_monthly_revenue (
    year                SMALLINT        NOT NULL,
    month               TINYINT         NOT NULL,
    bill_count          INT             NOT NULL DEFAULT 0,
    total_revenue       DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_revenue         DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    mom_growth_pct      DECIMAL(6,2)    DEFAULT NULL COMMENT 'Month-over-month growth %',
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 6. Treatment Cost by Type ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_treatment_cost (
    treatment_type      VARCHAR(100)    NOT NULL,
    treatment_count     INT             NOT NULL DEFAULT 0,
    avg_cost            DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    min_cost            DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    max_cost            DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    total_cost          DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (treatment_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 7. Revenue by Specialization ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_specialization (
    specialization      VARCHAR(100)    NOT NULL,
    doctor_count        INT             NOT NULL DEFAULT 0,
    total_appointments  INT             NOT NULL DEFAULT 0,
    total_revenue       DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_revenue_per_doc DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    avg_revenue_per_appt DECIMAL(10,2)  NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (specialization)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 8. Doctor Workload ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_doctor_workload (
    doctor_id               VARCHAR(10)     NOT NULL,
    full_name               VARCHAR(101)    NOT NULL,
    specialization          VARCHAR(100)    NOT NULL,
    hospital_branch         VARCHAR(100),
    total_appointments      INT             NOT NULL DEFAULT 0,
    completed_appointments  INT             NOT NULL DEFAULT 0,
    unique_patients         INT             NOT NULL DEFAULT 0,
    no_show_count           INT             NOT NULL DEFAULT 0,
    cancellation_count      INT             NOT NULL DEFAULT 0,
    no_show_rate_pct        DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    cancellation_rate_pct   DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    completion_rate_pct     DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    last_updated            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (doctor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 9. Peak Appointment Hours ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_peak_hours (
    hour_of_day         TINYINT         NOT NULL COMMENT '0-23',
    appointment_count   INT             NOT NULL DEFAULT 0,
    completed_count     INT             NOT NULL DEFAULT 0,
    no_show_count       INT             NOT NULL DEFAULT 0,
    completion_rate_pct DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (hour_of_day)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 10. Patient Age Group Analysis ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_age_groups (
    age_group           VARCHAR(20)     NOT NULL COMMENT '0-18, 19-35, 36-50, 51-65, 65+',
    patient_count       INT             NOT NULL DEFAULT 0,
    total_appointments  INT             NOT NULL DEFAULT 0,
    total_spend         DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_spend           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    most_common_reason  VARCHAR(255),
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (age_group)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 11. Patient Retention ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_patient_retention (
    visit_segment       VARCHAR(20)     NOT NULL COMMENT 'single_visit, 2-3_visits, 4-6_visits, 7+_visits',
    patient_count       INT             NOT NULL DEFAULT 0,
    pct_of_patients     DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    avg_spend           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    total_revenue       DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (visit_segment)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 12. Monthly New Patient Registrations ────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_new_patient_trend (
    year                SMALLINT        NOT NULL,
    month               TINYINT         NOT NULL,
    new_patients        INT             NOT NULL DEFAULT 0,
    male_count          INT             NOT NULL DEFAULT 0,
    female_count        INT             NOT NULL DEFAULT 0,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (year, month)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 13. Pending & Failed Payments Outstanding ────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_outstanding_payments (
    payment_status      VARCHAR(20)     NOT NULL,
    bill_count          INT             NOT NULL DEFAULT 0,
    total_outstanding   DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_outstanding     DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    oldest_bill_date    DATE,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (payment_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 14. Revenue by Hospital Branch ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_revenue_by_branch (
    hospital_branch     VARCHAR(100)    NOT NULL,
    doctor_count        INT             NOT NULL DEFAULT 0,
    total_appointments  INT             NOT NULL DEFAULT 0,
    total_revenue       DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    avg_revenue_per_appt DECIMAL(10,2)  NOT NULL DEFAULT 0.00,
    last_updated        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (hospital_branch)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── 15. Top Doctors Scorecard ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analytics_top_doctors_scorecard (
    doctor_id               VARCHAR(10)     NOT NULL,
    full_name               VARCHAR(101)    NOT NULL,
    specialization          VARCHAR(100)    NOT NULL,
    hospital_branch         VARCHAR(100),
    total_revenue           DECIMAL(12,2)   NOT NULL DEFAULT 0.00,
    completion_rate_pct     DECIMAL(5,2)    NOT NULL DEFAULT 0.00,
    unique_patients         INT             NOT NULL DEFAULT 0,
    revenue_rank            INT             NOT NULL DEFAULT 0,
    completion_rank         INT             NOT NULL DEFAULT 0,
    overall_score           DECIMAL(8,2)    NOT NULL DEFAULT 0.00 COMMENT 'Composite score (revenue + completion)',
    last_updated            TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (doctor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
