from fastapi import APIRouter
import db

router = APIRouter()


@router.get("/age-groups")
def age_groups():
    return db.query("SELECT * FROM analytics_patient_age_groups ORDER BY age_group")


@router.get("/retention")
def retention():
    return db.query("SELECT * FROM analytics_patient_retention ORDER BY visit_segment")


@router.get("/new-patient-trend")
def new_patient_trend():
    return db.query("SELECT * FROM analytics_new_patient_trend ORDER BY year, month")


@router.get("/spending")
def spending():
    return db.query("SELECT * FROM analytics_patient_spending ORDER BY total_spend DESC")


@router.get("/summary")
def patients_summary():
    rows = db.query("""
        SELECT
            (SELECT COALESCE(SUM(patient_count),0) FROM analytics_patient_age_groups)  AS total_patients,
            (SELECT COALESCE(SUM(new_patients),0) FROM analytics_new_patient_trend
             WHERE year = YEAR(CURDATE()) AND month = MONTH(CURDATE()))                AS new_this_month,
            (SELECT COALESCE(AVG(avg_spend),0) FROM analytics_patient_spending)        AS avg_spend,
            (SELECT COALESCE(SUM(CASE WHEN visit_segment != 'single_visit'
                THEN patient_count ELSE 0 END),0) /
             NULLIF(SUM(patient_count),0) * 100
             FROM analytics_patient_retention)                                         AS retention_pct
    """)
    r = rows[0]
    r["retention_pct"] = round(r["retention_pct"] or 0, 1)
    r["avg_spend"] = round(r["avg_spend"] or 0, 2)
    return r
