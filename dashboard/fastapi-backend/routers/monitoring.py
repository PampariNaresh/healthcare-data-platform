from fastapi import APIRouter
import db

router = APIRouter()


@router.get("/summary")
def monitoring_summary():
    rows = db.query("""
        SELECT
            (SELECT COALESCE(SUM(anomaly_count),    0)    FROM analytics_vitals_patient_summary) AS total_anomalies,
            (SELECT COALESCE(ROUND(AVG(anomaly_rate_pct),1), 0) FROM analytics_vitals_patient_summary) AS avg_anomaly_rate,
            (SELECT COALESCE(SUM(critical_count),   0)    FROM analytics_lab_test_summary)       AS critical_lab_tests,
            (SELECT COALESCE(SUM(code_count),        0)   FROM analytics_icu_code_summary)        AS icu_activations
    """)
    return rows[0]


@router.get("/vitals-summary")
def vitals_summary():
    return db.query("""
        SELECT patient_id, total_readings, anomaly_count, anomaly_rate_pct,
               avg_heart_rate, avg_spo2, avg_systolic, avg_diastolic,
               avg_temperature, avg_respiratory_rate
        FROM analytics_vitals_patient_summary
        ORDER BY anomaly_rate_pct DESC
    """)


@router.get("/lab-tests")
def lab_tests():
    return db.query("""
        SELECT test_name, total_tests, normal_count, low_count, high_count,
               critical_count, critical_rate_pct, avg_amount, total_revenue
        FROM analytics_lab_test_summary
        ORDER BY total_tests DESC
    """)


@router.get("/hospital-events")
def hospital_events():
    return db.query("""
        SELECT event_type, event_count, total_amount, avg_amount
        FROM analytics_hospital_event_summary
        ORDER BY event_count DESC
    """)


@router.get("/department-activity")
def department_activity():
    return db.query("""
        SELECT department_id, department_name, hospital_branch,
               total_events, total_icu_codes, critical_icu_count,
               total_event_amount, total_icu_amount, total_amount
        FROM analytics_department_activity
        ORDER BY total_amount DESC
    """)


@router.get("/icu-codes")
def icu_codes():
    return db.query("""
        SELECT code_type, severity, code_count, total_amount, avg_amount
        FROM analytics_icu_code_summary
        ORDER BY code_count DESC
    """)
