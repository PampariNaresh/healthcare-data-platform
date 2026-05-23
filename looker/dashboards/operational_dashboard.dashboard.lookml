- dashboard: operational_dashboard
  title: Operational Dashboard
  layout: newspaper
  preferred_viewer: dashboards-next

  filters:
  - name: specialization
    title: Specialization
    type: field_filter
    explore: analytics_doctor_workload
    field: analytics_doctor_workload.specialization
    default_value: ''
  - name: hospital_branch
    title: Hospital Branch
    type: field_filter
    explore: analytics_doctor_workload
    field: analytics_doctor_workload.hospital_branch
    default_value: ''

  elements:
  - title: Total Appointments
    name: total_appts_kpi
    model: healthcare
    explore: analytics_doctor_workload
    type: single_value
    fields: [analytics_doctor_workload.total_appointments]
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Avg Completion Rate
    name: completion_kpi
    model: healthcare
    explore: analytics_doctor_workload
    type: single_value
    fields: [analytics_doctor_workload.completion_rate_pct]
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Avg No-Show Rate
    name: noshow_kpi
    model: healthcare
    explore: analytics_doctor_workload
    type: single_value
    fields: [analytics_doctor_workload.no_show_rate_pct]
    row: 0
    col: 12
    width: 6
    height: 4

  - title: Avg Cancellation Rate
    name: cancel_kpi
    model: healthcare
    explore: analytics_doctor_workload
    type: single_value
    fields: [analytics_doctor_workload.cancellation_rate_pct]
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Top Doctors Scorecard
    name: scorecard
    model: healthcare
    explore: analytics_top_doctors_scorecard
    type: looker_grid
    fields: [analytics_top_doctors_scorecard.full_name, analytics_top_doctors_scorecard.specialization, analytics_top_doctors_scorecard.hospital_branch, analytics_top_doctors_scorecard.total_revenue, analytics_top_doctors_scorecard.completion_rate_pct, analytics_top_doctors_scorecard.unique_patients, analytics_top_doctors_scorecard.overall_score, analytics_top_doctors_scorecard.revenue_rank]
    sorts: [analytics_top_doctors_scorecard.revenue_rank asc]
    limit: 20
    row: 4
    col: 0
    width: 24
    height: 10

  - title: Doctor Workload — Appointments & Rates
    name: doctor_workload
    model: healthcare
    explore: analytics_doctor_workload
    type: looker_bar
    fields: [analytics_doctor_workload.full_name, analytics_doctor_workload.total_appointments, analytics_doctor_workload.completed_appointments, analytics_doctor_workload.no_show_count]
    sorts: [analytics_doctor_workload.total_appointments desc]
    limit: 15
    row: 14
    col: 0
    width: 14
    height: 8

  - title: Appointment Status by Doctor
    name: appt_status
    model: healthcare
    explore: analytics_appointment_status
    type: looker_bar
    fields: [analytics_appointment_status.full_name, analytics_appointment_status.appt_status, analytics_appointment_status.status_count]
    sorts: [analytics_appointment_status.status_count desc]
    limit: 20
    row: 14
    col: 14
    width: 10
    height: 8

  - title: Peak Appointment Hours
    name: peak_hours
    model: healthcare
    explore: analytics_peak_hours
    type: looker_column
    fields: [analytics_peak_hours.hour_label, analytics_peak_hours.appointment_count, analytics_peak_hours.completed_count, analytics_peak_hours.no_show_count]
    sorts: [analytics_peak_hours.hour_of_day asc]
    row: 22
    col: 0
    width: 14
    height: 8

  - title: Completion Rate by Hour
    name: completion_by_hour
    model: healthcare
    explore: analytics_peak_hours
    type: looker_line
    fields: [analytics_peak_hours.hour_label, analytics_peak_hours.completion_rate_pct]
    sorts: [analytics_peak_hours.hour_of_day asc]
    row: 22
    col: 14
    width: 10
    height: 8
