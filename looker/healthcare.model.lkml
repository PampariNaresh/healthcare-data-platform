connection: "healthcare-mysql"

include: "/views/*.view.lkml"
include: "/dashboards/*.dashboard.lookml"

# ── Financial Explores ────────────────────────────────────────────────────────
explore: analytics_revenue_by_doctor {
  label: "Revenue by Doctor"
  group_label: "Financial"
}

explore: analytics_revenue_by_specialization {
  label: "Revenue by Specialization"
  group_label: "Financial"
}

explore: analytics_revenue_by_branch {
  label: "Revenue by Branch"
  group_label: "Financial"
}

explore: analytics_billing_payment {
  label: "Billing & Payment"
  group_label: "Financial"
}

explore: analytics_outstanding_payments {
  label: "Outstanding Payments"
  group_label: "Financial"
}

explore: analytics_monthly_revenue {
  label: "Monthly Revenue"
  group_label: "Financial"
}

explore: analytics_treatment_cost {
  label: "Treatment Cost"
  group_label: "Financial"
}

# ── Operational Explores ──────────────────────────────────────────────────────
explore: analytics_appointment_status {
  label: "Appointment Status"
  group_label: "Operational"
}

explore: analytics_doctor_workload {
  label: "Doctor Workload"
  group_label: "Operational"
}

explore: analytics_peak_hours {
  label: "Peak Hours"
  group_label: "Operational"
}

explore: analytics_top_doctors_scorecard {
  label: "Top Doctors Scorecard"
  group_label: "Operational"
}

# ── Patient Explores ──────────────────────────────────────────────────────────
explore: analytics_patient_spending {
  label: "Patient Spending"
  group_label: "Patient"
}

explore: analytics_patient_age_groups {
  label: "Patient Age Groups"
  group_label: "Patient"
}

explore: analytics_patient_retention {
  label: "Patient Retention"
  group_label: "Patient"
}

explore: analytics_new_patient_trend {
  label: "New Patient Trend"
  group_label: "Patient"
}
