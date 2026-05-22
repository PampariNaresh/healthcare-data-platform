from fastapi import APIRouter
import db

router = APIRouter()


@router.get("/doctor-workload")
def doctor_workload():
    return db.query("SELECT * FROM analytics_doctor_workload ORDER BY total_appointments DESC")


@router.get("/appointment-status")
def appointment_status():
    return db.query("SELECT * FROM analytics_appointment_status ORDER BY doctor_id, status")


@router.get("/peak-hours")
def peak_hours():
    return db.query("SELECT * FROM analytics_peak_hours ORDER BY hour_of_day")


@router.get("/top-doctors-scorecard")
def top_doctors_scorecard():
    return db.query("SELECT * FROM analytics_top_doctors_scorecard ORDER BY overall_score DESC")


@router.get("/summary")
def operational_summary():
    rows = db.query("""
        SELECT
            (SELECT COALESCE(SUM(total_appointments),0) FROM analytics_doctor_workload)       AS total_appointments,
            (SELECT COALESCE(SUM(completed_appointments),0) FROM analytics_doctor_workload)   AS completed,
            (SELECT COALESCE(SUM(no_show_count),0) FROM analytics_doctor_workload)            AS no_shows,
            (SELECT COALESCE(SUM(cancellation_count),0) FROM analytics_doctor_workload)       AS cancellations
    """)
    r = rows[0]
    total = r["total_appointments"] or 1
    return {
        **r,
        "completion_rate_pct": round(r["completed"] / total * 100, 1),
        "no_show_rate_pct":    round(r["no_shows"]  / total * 100, 1),
        "cancellation_rate_pct": round(r["cancellations"] / total * 100, 1),
    }
