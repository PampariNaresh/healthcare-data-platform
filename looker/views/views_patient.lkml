# ── analytics_patient_spending ────────────────────────────────────────────────
view: analytics_patient_spending {
  sql_table_name: healthcare.analytics_patient_spending ;;

  dimension: insurance_provider {
    type: string
    primary_key: yes
    sql: ${TABLE}.insurance_provider ;;
  }
  measure: patient_count {
    type: sum
    sql: ${TABLE}.patient_count ;;
  }
  measure: avg_age {
    type: average
    sql: ${TABLE}.avg_age ;;
    value_format_name: decimal_1
  }
  measure: male_count {
    type: sum
    sql: ${TABLE}.male_count ;;
  }
  measure: female_count {
    type: sum
    sql: ${TABLE}.female_count ;;
  }
  measure: avg_spend {
    type: average
    sql: ${TABLE}.avg_spend ;;
    value_format_name: decimal_2
  }
  measure: total_spend {
    type: sum
    sql: ${TABLE}.total_spend ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_patient_age_groups ──────────────────────────────────────────────
view: analytics_patient_age_groups {
  sql_table_name: healthcare.analytics_patient_age_groups ;;

  dimension: age_group {
    type: string
    primary_key: yes
    sql: ${TABLE}.age_group ;;
  }
  measure: patient_count {
    type: sum
    sql: ${TABLE}.patient_count ;;
  }
  measure: total_appointments {
    type: sum
    sql: ${TABLE}.total_appointments ;;
  }
  measure: total_spend {
    type: sum
    sql: ${TABLE}.total_spend ;;
    value_format_name: decimal_2
  }
  measure: avg_spend {
    type: average
    sql: ${TABLE}.avg_spend ;;
    value_format_name: decimal_2
  }
  dimension: most_common_reason {
    type: string
    sql: ${TABLE}.most_common_reason ;;
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_patient_retention ───────────────────────────────────────────────
view: analytics_patient_retention {
  sql_table_name: healthcare.analytics_patient_retention ;;

  dimension: visit_segment {
    type: string
    primary_key: yes
    sql: ${TABLE}.visit_segment ;;
  }
  measure: patient_count {
    type: sum
    sql: ${TABLE}.patient_count ;;
  }
  measure: pct_of_patients {
    type: average
    label: "% of Patients"
    sql: ${TABLE}.pct_of_patients ;;
    value_format_name: percent_2
  }
  measure: avg_spend {
    type: average
    sql: ${TABLE}.avg_spend ;;
    value_format_name: decimal_2
  }
  measure: total_revenue {
    type: sum
    sql: ${TABLE}.total_revenue ;;
    value_format_name: decimal_2
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}

# ── analytics_new_patient_trend ───────────────────────────────────────────────
view: analytics_new_patient_trend {
  sql_table_name: healthcare.analytics_new_patient_trend ;;

  dimension: trend_year {
    type: number
    sql: ${TABLE}.trend_year ;;
  }
  dimension: trend_month {
    type: number
    sql: ${TABLE}.trend_month ;;
  }
  dimension: year_month {
    type: string
    primary_key: yes
    sql: CONCAT(${TABLE}.trend_year, '-', LPAD(${TABLE}.trend_month, 2, '0')) ;;
  }
  measure: new_patients {
    type: sum
    sql: ${TABLE}.new_patients ;;
  }
  measure: male_count {
    type: sum
    sql: ${TABLE}.male_count ;;
  }
  measure: female_count {
    type: sum
    sql: ${TABLE}.female_count ;;
  }
  dimension_group: last_updated {
    type: time
    sql: ${TABLE}.last_updated ;;
  }
}
