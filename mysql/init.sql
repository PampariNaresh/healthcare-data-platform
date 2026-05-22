-- ─────────────────────────────────────────────────────────────────────────────
-- Healthcare Database Schema
-- ─────────────────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS healthcare
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE healthcare;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: patients
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS patients (
    patient_id          VARCHAR(10)      NOT NULL,
    first_name          VARCHAR(50)      NOT NULL,
    last_name           VARCHAR(50)      NOT NULL,
    gender              CHAR(1)          NOT NULL,
    date_of_birth       DATE             NOT NULL,
    contact_number      VARCHAR(15)      NOT NULL,
    address             VARCHAR(255)     DEFAULT NULL,
    registration_date   DATE             NOT NULL,
    insurance_provider  VARCHAR(100)     DEFAULT NULL,
    insurance_number    VARCHAR(20)      DEFAULT NULL,
    email               VARCHAR(100)     DEFAULT NULL,
    PRIMARY KEY (patient_id),
    UNIQUE KEY uq_patient_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: doctors
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS doctors (
    doctor_id           VARCHAR(10)      NOT NULL,
    first_name          VARCHAR(50)      NOT NULL,
    last_name           VARCHAR(50)      NOT NULL,
    specialization      VARCHAR(100)     NOT NULL,
    phone_number        VARCHAR(15)      NOT NULL,
    years_experience    TINYINT UNSIGNED NOT NULL DEFAULT 0,
    hospital_branch     VARCHAR(100)     DEFAULT NULL,
    email               VARCHAR(100)     DEFAULT NULL,
    PRIMARY KEY (doctor_id),
    UNIQUE KEY uq_doctor_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: appointments
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS appointments (
    appointment_id      VARCHAR(10)      NOT NULL,
    patient_id          VARCHAR(10)      NOT NULL,
    doctor_id           VARCHAR(10)      NOT NULL,
    appointment_date    DATE             NOT NULL,
    appointment_time    TIME             NOT NULL,
    reason_for_visit    VARCHAR(255)     DEFAULT NULL,
    status              VARCHAR(20)      NOT NULL,
    PRIMARY KEY (appointment_id),
    KEY idx_appt_patient  (patient_id),
    KEY idx_appt_doctor   (doctor_id),
    KEY idx_appt_date     (appointment_date),
    CONSTRAINT fk_appt_patient FOREIGN KEY (patient_id)
        REFERENCES patients (patient_id) ON DELETE CASCADE,
    CONSTRAINT fk_appt_doctor  FOREIGN KEY (doctor_id)
        REFERENCES doctors  (doctor_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: treatments
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS treatments (
    treatment_id        VARCHAR(10)      NOT NULL,
    appointment_id      VARCHAR(10)      NOT NULL,
    treatment_type      VARCHAR(100)     NOT NULL,
    description         VARCHAR(255)     DEFAULT NULL,
    cost                DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    treatment_date      DATE             NOT NULL,
    PRIMARY KEY (treatment_id),
    KEY idx_treat_appointment (appointment_id),
    KEY idx_treat_date        (treatment_date),
    CONSTRAINT fk_treat_appointment FOREIGN KEY (appointment_id)
        REFERENCES appointments (appointment_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ─────────────────────────────────────────────────────────────────────────────
-- Table: billing
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS billing (
    bill_id             VARCHAR(10)      NOT NULL,
    patient_id          VARCHAR(10)      NOT NULL,
    treatment_id        VARCHAR(10)      NOT NULL,
    bill_date           DATE             NOT NULL,
    amount              DECIMAL(10, 2)   NOT NULL DEFAULT 0.00,
    payment_method      VARCHAR(50)      DEFAULT NULL,
    payment_status      VARCHAR(20)      NOT NULL,
    PRIMARY KEY (bill_id),
    KEY idx_bill_patient   (patient_id),
    KEY idx_bill_treatment (treatment_id),
    KEY idx_bill_date      (bill_date),
    CONSTRAINT fk_bill_patient   FOREIGN KEY (patient_id)
        REFERENCES patients   (patient_id) ON DELETE CASCADE,
    CONSTRAINT fk_bill_treatment FOREIGN KEY (treatment_id)
        REFERENCES treatments (treatment_id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
