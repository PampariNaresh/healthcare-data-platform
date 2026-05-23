- dashboard: patient_dashboard
  title: Patient Dashboard
  layout: newspaper
  preferred_viewer: dashboards-next

  filters:
  - name: insurance_provider
    title: Insurance Provider
    type: field_filter
    explore: analytics_patient_spending
    field: analytics_patient_spending.insurance_provider
    default_value: ''

  elements:
  - title: Total Patients
    name: total_patients_kpi
    model: healthcare
    explore: analytics_patient_spending
    type: single_value
    fields: [analytics_patient_spending.patient_count]
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Avg Patient Spend
    name: avg_spend_kpi
    model: healthcare
    explore: analytics_patient_spending
    type: single_value
    fields: [analytics_patient_spending.avg_spend]
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Total Patient Revenue
    name: total_spend_kpi
    model: healthcare
    explore: analytics_patient_spending
    type: single_value
    fields: [analytics_patient_spending.total_spend]
    row: 0
    col: 12
    width: 6
    height: 4

  - title: New Patients This Year
    name: new_patients_kpi
    model: healthcare
    explore: analytics_new_patient_trend
    type: single_value
    fields: [analytics_new_patient_trend.new_patients]
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Spending by Insurance Provider
    name: insurance_spending
    model: healthcare
    explore: analytics_patient_spending
    type: looker_bar
    fields: [analytics_patient_spending.insurance_provider, analytics_patient_spending.total_spend, analytics_patient_spending.patient_count]
    sorts: [analytics_patient_spending.total_spend desc]
    row: 4
    col: 0
    width: 12
    height: 8

  - title: Patient Retention Segments
    name: retention
    model: healthcare
    explore: analytics_patient_retention
    type: looker_pie
    fields: [analytics_patient_retention.visit_segment, analytics_patient_retention.patient_count]
    sorts: [analytics_patient_retention.patient_count desc]
    row: 4
    col: 12
    width: 12
    height: 8

  - title: Spend by Age Group
    name: age_group_spend
    model: healthcare
    explore: analytics_patient_age_groups
    type: looker_column
    fields: [analytics_patient_age_groups.age_group, analytics_patient_age_groups.avg_spend, analytics_patient_age_groups.patient_count]
    sorts: [analytics_patient_age_groups.age_group asc]
    row: 12
    col: 0
    width: 12
    height: 8

  - title: Most Common Visit Reason by Age Group
    name: visit_reason
    model: healthcare
    explore: analytics_patient_age_groups
    type: looker_grid
    fields: [analytics_patient_age_groups.age_group, analytics_patient_age_groups.most_common_reason, analytics_patient_age_groups.total_appointments, analytics_patient_age_groups.avg_spend]
    sorts: [analytics_patient_age_groups.total_appointments desc]
    row: 12
    col: 12
    width: 12
    height: 8

  - title: New Patient Trend (Monthly)
    name: new_patient_trend
    model: healthcare
    explore: analytics_new_patient_trend
    type: looker_line
    fields: [analytics_new_patient_trend.year_month, analytics_new_patient_trend.new_patients, analytics_new_patient_trend.male_count, analytics_new_patient_trend.female_count]
    sorts: [analytics_new_patient_trend.year_month asc]
    row: 20
    col: 0
    width: 14
    height: 8

  - title: Gender Split by Insurance Provider
    name: gender_split
    model: healthcare
    explore: analytics_patient_spending
    type: looker_column
    fields: [analytics_patient_spending.insurance_provider, analytics_patient_spending.male_count, analytics_patient_spending.female_count]
    sorts: [analytics_patient_spending.patient_count desc]
    row: 20
    col: 14
    width: 10
    height: 8
