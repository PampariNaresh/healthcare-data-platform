from fastapi import APIRouter
import db

router = APIRouter()


@router.get("/revenue-by-doctor")
def revenue_by_doctor():
    return db.query("SELECT * FROM analytics_revenue_by_doctor ORDER BY total_revenue DESC")


@router.get("/revenue-by-specialization")
def revenue_by_specialization():
    return db.query("SELECT * FROM analytics_revenue_by_specialization ORDER BY total_revenue DESC")


@router.get("/revenue-by-branch")
def revenue_by_branch():
    return db.query("SELECT * FROM analytics_revenue_by_branch ORDER BY total_revenue DESC")


@router.get("/monthly-revenue")
def monthly_revenue():
    return db.query("SELECT * FROM analytics_monthly_revenue ORDER BY year, month")


@router.get("/billing-payment")
def billing_payment():
    return db.query("SELECT * FROM analytics_billing_payment ORDER BY total_amount DESC")


@router.get("/treatment-cost")
def treatment_cost():
    return db.query("SELECT * FROM analytics_treatment_cost ORDER BY total_cost DESC")


@router.get("/outstanding-payments")
def outstanding_payments():
    return db.query("SELECT * FROM analytics_outstanding_payments")


@router.get("/summary")
def financial_summary():
    rows = db.query("""
        SELECT
            (SELECT COALESCE(SUM(total_revenue),0) FROM analytics_revenue_by_doctor)          AS total_revenue,
            (SELECT COALESCE(SUM(total_outstanding),0) FROM analytics_outstanding_payments)   AS total_outstanding,
            (SELECT COALESCE(SUM(bill_count),0) FROM analytics_billing_payment)               AS total_bills,
            (SELECT COALESCE(SUM(bill_count),0) FROM analytics_billing_payment
             WHERE payment_status = 'Paid')                                                   AS paid_bills
    """)
    r = rows[0]
    paid_pct = round(r["paid_bills"] / r["total_bills"] * 100, 1) if r["total_bills"] else 0
    return {**r, "paid_pct": paid_pct}
