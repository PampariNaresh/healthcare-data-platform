# ── analytics_appointment_status ──────────────────────────────────────────────
view: analytics_appointment_status {
  sql_table_name: healthcare.analytics_appointment_status ;;

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
  dimension: appt_status {
    type: string
    label: "Appointment Status"
    sql: ${TABLE}.appt_status ;;
  }
  measure: status_count {
    type: sum
    label: "Count"
    sql: ${TABLE}.status_count ;;
  }
  measure: pct_of_total {
    type: average
    label: "% of Total"
    sql: ${TABLE}.pct_of_total ;;
    value_format_name: percent_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_doctor_workload ─────────────────────────────────────────────────
view: analytics_doctor_workload {
  sql_table_name: healthcare.analytics_doctor_workload ;;

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
  measure: total_appointments {
    type: sum
    sql: ${TABLE}.total_appointments ;;
  }
  measure: completed_appointments {
    type: sum
    sql: ${TABLE}.completed_appointments ;;
  }
  measure: unique_patients {
    type: sum
    sql: ${TABLE}.unique_patients ;;
  }
  measure: no_show_count {
    type: sum
    sql: ${TABLE}.no_show_count ;;
  }
  measure: cancellation_count {
    type: sum
    sql: ${TABLE}.cancellation_count ;;
  }
  measure: no_show_rate_pct {
    type: average
    label: "No-Show Rate %"
    sql: ${TABLE}.no_show_rate_pct ;;
    value_format_name: decimal_1
  }
  measure: cancellation_rate_pct {
    type: average
    label: "Cancellation Rate %"
    sql: ${TABLE}.cancellation_rate_pct ;;
    value_format_name: decimal_1
  }
  measure: completion_rate_pct {
    type: average
    label: "Completion Rate %"
    sql: ${TABLE}.completion_rate_pct ;;
    value_format_name: decimal_1
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_peak_hours ──────────────────────────────────────────────────────
view: analytics_peak_hours {
  sql_table_name: healthcare.analytics_peak_hours ;;

  dimension: hour_of_day {
    type: number
    primary_key: yes
    sql: ${TABLE}.hour_of_day ;;
  }
  dimension: hour_label {
    type: string
    label: "Hour"
    sql: CONCAT(LPAD(${TABLE}.hour_of_day, 2, '0'), ':00') ;;
  }
  measure: appointment_count {
    type: sum
    sql: ${TABLE}.appointment_count ;;
  }
  measure: completed_count {
    type: sum
    sql: ${TABLE}.completed_count ;;
  }
  measure: no_show_count {
    type: sum
    sql: ${TABLE}.no_show_count ;;
  }
  measure: completion_rate_pct {
    type: average
    label: "Completion Rate %"
    sql: ${TABLE}.completion_rate_pct ;;
    value_format_name: decimal_1
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_top_doctors_scorecard ───────────────────────────────────────────
view: analytics_top_doctors_scorecard {
  sql_table_name: healthcare.analytics_top_doctors_scorecard ;;

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
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  measure: completion_rate_pct {
    type: average
    label: "Completion Rate %"
    sql: ${TABLE}.completion_rate_pct ;;
    value_format_name: decimal_1
  }
  measure: unique_patients {
    type: sum
    sql: ${TABLE}.unique_patients ;;
  }
  dimension: revenue_rank {
    type: number
    sql: ${TABLE}.revenue_rank ;;
  }
  dimension: completion_rank {
    type: number
    sql: ${TABLE}.completion_rank ;;
  }
  measure: overall_score {
    type: average
    sql: ${TABLE}.overall_score ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}
