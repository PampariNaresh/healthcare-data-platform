- dashboard: financial_dashboard
  title: Financial Dashboard
  layout: newspaper
  preferred_viewer: dashboards-next

  filters:
  - name: hospital_branch
    title: Hospital Branch
    type: field_filter
    explore: analytics_revenue_by_doctor
    field: analytics_revenue_by_doctor.hospital_branch
    default_value: ''
  - name: specialization
    title: Specialization
    type: field_filter
    explore: analytics_revenue_by_doctor
    field: analytics_revenue_by_doctor.specialization
    default_value: ''

  elements:
  - title: Total Revenue
    name: total_revenue_kpi
    model: healthcare
    explore: analytics_revenue_by_doctor
    type: single_value
    fields: [analytics_revenue_by_doctor.total_revenue]
    row: 0
    col: 0
    width: 6
    height: 4

  - title: Total Bills
    name: total_bills_kpi
    model: healthcare
    explore: analytics_revenue_by_doctor
    type: single_value
    fields: [analytics_revenue_by_doctor.total_bills]
    row: 0
    col: 6
    width: 6
    height: 4

  - title: Avg Bill Amount
    name: avg_bill_kpi
    model: healthcare
    explore: analytics_revenue_by_doctor
    type: single_value
    fields: [analytics_revenue_by_doctor.avg_bill_amount]
    row: 0
    col: 12
    width: 6
    height: 4

  - title: Outstanding Amount
    name: outstanding_kpi
    model: healthcare
    explore: analytics_outstanding_payments
    type: single_value
    fields: [analytics_outstanding_payments.total_outstanding]
    row: 0
    col: 18
    width: 6
    height: 4

  - title: Revenue by Doctor (Top 10)
    name: revenue_by_doctor
    model: healthcare
    explore: analytics_revenue_by_doctor
    type: looker_bar
    fields: [analytics_revenue_by_doctor.full_name, analytics_revenue_by_doctor.total_revenue]
    sorts: [analytics_revenue_by_doctor.total_revenue desc]
    limit: 10
    row: 4
    col: 0
    width: 12
    height: 8

  - title: Revenue by Specialization
    name: revenue_by_spec
    model: healthcare
    explore: analytics_revenue_by_specialization
    type: looker_pie
    fields: [analytics_revenue_by_specialization.specialization, analytics_revenue_by_specialization.total_revenue]
    sorts: [analytics_revenue_by_specialization.total_revenue desc]
    row: 4
    col: 12
    width: 12
    height: 8

  - title: Revenue by Hospital Branch
    name: revenue_by_branch
    model: healthcare
    explore: analytics_revenue_by_branch
    type: looker_column
    fields: [analytics_revenue_by_branch.hospital_branch, analytics_revenue_by_branch.total_revenue, analytics_revenue_by_branch.avg_revenue_per_appt]
    sorts: [analytics_revenue_by_branch.total_revenue desc]
    row: 12
    col: 0
    width: 12
    height: 8

  - title: Monthly Revenue Trend
    name: monthly_revenue
    model: healthcare
    explore: analytics_monthly_revenue
    type: looker_line
    fields: [analytics_monthly_revenue.year_month, analytics_monthly_revenue.total_revenue, analytics_monthly_revenue.mom_growth_pct]
    sorts: [analytics_monthly_revenue.year_month asc]
    row: 12
    col: 12
    width: 12
    height: 8

  - title: Payment Method Breakdown
    name: payment_method
    model: healthcare
    explore: analytics_billing_payment
    type: looker_pie
    fields: [analytics_billing_payment.payment_method, analytics_billing_payment.total_amount]
    sorts: [analytics_billing_payment.total_amount desc]
    row: 20
    col: 0
    width: 8
    height: 8

  - title: Outstanding Payments by Status
    name: outstanding_status
    model: healthcare
    explore: analytics_outstanding_payments
    type: looker_column
    fields: [analytics_outstanding_payments.payment_status, analytics_outstanding_payments.total_outstanding, analytics_outstanding_payments.bill_count]
    row: 20
    col: 8
    width: 8
    height: 8

  - title: Treatment Cost Analysis
    name: treatment_cost
    model: healthcare
    explore: analytics_treatment_cost
    type: looker_bar
    fields: [analytics_treatment_cost.treatment_type, analytics_treatment_cost.avg_cost, analytics_treatment_cost.total_cost]
    sorts: [analytics_treatment_cost.total_cost desc]
    row: 20
    col: 16
    width: 8
    height: 8
