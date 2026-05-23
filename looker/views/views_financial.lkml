# ── analytics_revenue_by_doctor ───────────────────────────────────────────────
view: analytics_revenue_by_doctor {
  sql_table_name: healthcare.analytics_revenue_by_doctor ;;

  dimension: doctor_id {
    type: string
    primary_key: yes
    sql: ${TABLE}.doctor_id ;;
  }
  dimension: full_name {
    type: string
    label: "Doctor Name"
    sql: ${TABLE}.full_name ;;
  }
  dimension: specialization {
    type: string
    sql: ${TABLE}.specialization ;;
  }
  dimension: hospital_branch {
    type: string
    sql: ${TABLE}.hospital_branch ;;
  }
  measure: total_bills {
    type: sum
    sql: ${TABLE}.total_bills ;;
  }
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  measure: avg_bill_amount {
    type: average
    sql: ${TABLE}.avg_bill_amount ;;
    value_format_name: decimal_2
  }
  measure: max_bill_amount {
    type: max
    sql: ${TABLE}.max_bill_amount ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_revenue_by_specialization ───────────────────────────────────────
view: analytics_revenue_by_specialization {
  sql_table_name: healthcare.analytics_revenue_by_specialization ;;

  dimension: specialization {
    type: string
    primary_key: yes
    sql: ${TABLE}.specialization ;;
  }
  measure: doctor_count {
    type: sum
    sql: ${TABLE}.doctor_count ;;
  }
  measure: total_appointments {
    type: sum
    sql: ${TABLE}.total_appointments ;;
  }
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  measure: avg_revenue_per_doc {
    type: average
    sql: ${TABLE}.avg_revenue_per_doc ;;
    value_format_name: decimal_2
  }
  measure: avg_revenue_per_appt {
    type: average
    sql: ${TABLE}.avg_revenue_per_appt ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_revenue_by_branch ───────────────────────────────────────────────
view: analytics_revenue_by_branch {
  sql_table_name: healthcare.analytics_revenue_by_branch ;;

  dimension: hospital_branch {
    type: string
    primary_key: yes
    sql: ${TABLE}.hospital_branch ;;
  }
  measure: doctor_count {
    type: sum
    sql: ${TABLE}.doctor_count ;;
  }
  measure: total_appointments {
    type: sum
    sql: ${TABLE}.total_appointments ;;
  }
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  measure: avg_revenue_per_appt {
    type: average
    sql: ${TABLE}.avg_revenue_per_appt ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_billing_payment ─────────────────────────────────────────────────
view: analytics_billing_payment {
  sql_table_name: healthcare.analytics_billing_payment ;;

  dimension: payment_method {
    type: string
    primary_key: yes
    sql: ${TABLE}.payment_method ;;
  }
  dimension: payment_status {
    type: string
    sql: ${TABLE}.payment_status ;;
  }
  measure: bill_count {
    type: sum
    sql: ${TABLE}.bill_count ;;
  }
  measure: total_amount {
    type: sum
    sql: ${TABLE}.total_amount ;;
    value_format_name: decimal_2
  }
  measure: avg_amount {
    type: average
    sql: ${TABLE}.avg_amount ;;
    value_format_name: decimal_2
  }
  measure: pct_of_total_revenue {
    type: average
    sql: ${TABLE}.pct_of_total_revenue ;;
    value_format_name: percent_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_outstanding_payments ────────────────────────────────────────────
view: analytics_outstanding_payments {
  sql_table_name: healthcare.analytics_outstanding_payments ;;

  dimension: payment_status {
    type: string
    primary_key: yes
    sql: ${TABLE}.payment_status ;;
  }
  measure: bill_count {
    type: sum
    sql: ${TABLE}.bill_count ;;
  }
  measure: total_outstanding {
    type: sum
    sql: ${TABLE}.total_outstanding ;;
    value_format_name: decimal_2
  }
  measure: avg_outstanding {
    type: average
    sql: ${TABLE}.avg_outstanding ;;
    value_format_name: decimal_2
  }
  dimension: oldest_bill_date {
    type: date
    sql: ${TABLE}.oldest_bill_date ;;
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_monthly_revenue ─────────────────────────────────────────────────
view: analytics_monthly_revenue {
  sql_table_name: healthcare.analytics_monthly_revenue ;;

  dimension: rev_year {
    type: number
    sql: ${TABLE}.rev_year ;;
  }
  dimension: rev_month {
    type: number
    sql: ${TABLE}.rev_month ;;
  }
  dimension: year_month {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.rev_year, '-', LPAD(${TABLE}.rev_month, 2, '0')) ;;
  }
  measure: bill_count {
    type: sum
    sql: ${TABLE}.bill_count ;;
  }
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  measure: avg_revenue {
    type: average
    sql: ${TABLE}.avg_revenue ;;
    value_format_name: decimal_2
  }
  measure: mom_growth_pct {
    type: average
    label: "MoM Growth %"
    sql: ${TABLE}.mom_growth_pct ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_treatment_cost ──────────────────────────────────────────────────
view: analytics_treatment_cost {
  sql_table_name: healthcare.analytics_treatment_cost ;;

  dimension: treatment_type {
    type: string
    primary_key: yes
    sql: ${TABLE}.treatment_type ;;
  }
  measure: treatment_count {
    type: sum
    sql: ${TABLE}.treatment_count ;;
  }
  measure: avg_cost {
    type: average
    sql: ${TABLE}.avg_cost ;;
    value_format_name: decimal_2
  }
  measure: min_cost {
    type: min
    sql: ${TABLE}.min_cost ;;
    value_format_name: decimal_2
  }
  measure: max_cost {
    type: max
    sql: ${TABLE}.max_cost ;;
    value_format_name: decimal_2
  }
  measure: total_cost {
    type: sum
    sql: ${TABLE}.total_cost ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}
