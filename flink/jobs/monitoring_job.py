"""
Monitoring DataStream Pipeline
  Kafka topics → validate → MySQL tables
  Covers: departments, patient_vitals, lab_reports, hospital_events, icu_codes
  Invalid records go to side output (printed to Flink logs)
"""
import json
import logging
import re
from datetime import datetime
from typing import Optional, Tuple

from pyflink.common import Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.jdbc import (
    JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
)
from pyflink.datastream.functions import ProcessFunction

import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MYSQL_URL           = os.getenv("MYSQL_URL",
                          "jdbc:mysql://mysql:3306/healthcare"
                          "?useSSL=false&allowPublicKeyRetrieval=true"
                          "&serverTimezone=UTC&sessionVariables=foreign_key_checks=0")
MYSQL_USER          = os.getenv("MYSQL_USER",     "hc_user")
MYSQL_PASSWORD      = os.getenv("MYSQL_PASSWORD", "hc_pass")
CHECKPOINT_INTERVAL = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "10000"))

INVALID_TAG = OutputTag("invalid-records", Types.STRING())


# ── Type Infos ────────────────────────────────────────────────────────────────

DEPARTMENT_TYPE = Types.ROW_NAMED(
    ["department_id", "department_name", "hospital_branch"],
    [Types.STRING(), Types.STRING(), Types.STRING()]
)

VITALS_TYPE = Types.ROW_NAMED(
    ["event_id", "patient_id", "hospital", "ward",
     "heart_rate", "spo2", "systolic", "diastolic",
     "temperature_celsius", "respiratory_rate", "is_anomaly", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.INT(), Types.DOUBLE(), Types.INT(), Types.INT(),
     Types.DOUBLE(), Types.INT(), Types.BOOLEAN(), Types.SQL_TIMESTAMP()]
)

LAB_TYPE = Types.ROW_NAMED(
    ["report_id", "patient_id", "doctor_id", "hospital",
     "test_name", "value", "unit", "normal_range", "flag", "amount", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
)

HOSPITAL_EVENT_TYPE = Types.ROW_NAMED(
    ["event_id", "patient_id", "department_id", "hospital",
     "ward", "event_type", "amount", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.SQL_TIMESTAMP()]
)

ICU_CODE_TYPE = Types.ROW_NAMED(
    ["code_id", "patient_id", "department_id", "hospital",
     "ward", "code_type", "severity", "amount", "status", "ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.STRING(), Types.STRING(),
     Types.DOUBLE(), Types.STRING(), Types.SQL_TIMESTAMP()]
)


# ── SQL (upsert via ON DUPLICATE KEY UPDATE) ──────────────────────────────────

DEPARTMENT_SQL = """
    INSERT INTO departments (department_id, department_name, hospital_branch)
    VALUES (?, ?, ?)
    ON DUPLICATE KEY UPDATE
        department_name=VALUES(department_name),
        hospital_branch=VALUES(hospital_branch)
"""

VITALS_SQL = """
    INSERT INTO patient_vitals
        (event_id, patient_id, hospital, ward,
         heart_rate, spo2, systolic, diastolic,
         temperature_celsius, respiratory_rate, is_anomaly, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        heart_rate=VALUES(heart_rate), spo2=VALUES(spo2),
        systolic=VALUES(systolic), diastolic=VALUES(diastolic),
        temperature_celsius=VALUES(temperature_celsius),
        respiratory_rate=VALUES(respiratory_rate),
        is_anomaly=VALUES(is_anomaly)
"""

LAB_SQL = """
    INSERT INTO lab_reports
        (report_id, patient_id, doctor_id, hospital,
         test_name, value, unit, normal_range, flag, amount, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        test_name=VALUES(test_name), value=VALUES(value),
        unit=VALUES(unit), normal_range=VALUES(normal_range),
        flag=VALUES(flag), amount=VALUES(amount)
"""

HOSPITAL_EVENT_SQL = """
    INSERT INTO hospital_events
        (event_id, patient_id, department_id, hospital,
         ward, event_type, amount, ts)
    VALUES (?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        event_type=VALUES(event_type), amount=VALUES(amount)
"""

ICU_CODE_SQL = """
    INSERT INTO icu_codes
        (code_id, patient_id, department_id, hospital,
         ward, code_type, severity, amount, status, ts)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        code_type=VALUES(code_type), severity=VALUES(severity),
        amount=VALUES(amount), status=VALUES(status)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


# ── Validators ────────────────────────────────────────────────────────────────

def validate_department(d: dict) -> Tuple[bool, str]:
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("department_name"):
        return False, "missing department_name"
    return True, ""

def validate_vitals(d: dict) -> Tuple[bool, str]:
    if not d.get("event_id"):
        return False, "missing event_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    try:
        hr = int(d.get("heart_rate", 0))
        if not (20 <= hr <= 300):
            return False, f"heart_rate out of range: {hr}"
    except (ValueError, TypeError):
        return False, f"invalid heart_rate '{d.get('heart_rate')}'"
    try:
        spo2 = float(d.get("spo2", -1))
        if not (0.0 <= spo2 <= 100.0):
            return False, f"spo2 out of range: {spo2}"
    except (ValueError, TypeError):
        return False, f"invalid spo2 '{d.get('spo2')}'"
    try:
        temp = float(d.get("temperature_celsius", 0))
        if not (30.0 <= temp <= 45.0):
            return False, f"temperature_celsius out of range: {temp}"
    except (ValueError, TypeError):
        return False, f"invalid temperature_celsius '{d.get('temperature_celsius')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_lab(d: dict) -> Tuple[bool, str]:
    if not d.get("report_id"):
        return False, "missing report_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("doctor_id"):
        return False, "missing doctor_id"
    if not d.get("test_name"):
        return False, "missing test_name"
    try:
        value = float(d.get("value", -1))
        if value < 0:
            return False, f"value must be >= 0, got {value}"
    except (ValueError, TypeError):
        return False, f"invalid value '{d.get('value')}'"
    valid_flags = {"normal", "low", "high", "critical"}
    if d.get("flag") not in valid_flags:
        return False, f"invalid flag '{d.get('flag')}' — must be one of {valid_flags}"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_hospital_event(d: dict) -> Tuple[bool, str]:
    if not d.get("event_id"):
        return False, "missing event_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("event_type"):
        return False, "missing event_type"
    try:
        amount = float(d.get("amount", -1))
        if amount < 0:
            return False, f"amount must be >= 0, got {amount}"
    except (ValueError, TypeError):
        return False, f"invalid amount '{d.get('amount')}'"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""

def validate_icu_code(d: dict) -> Tuple[bool, str]:
    if not d.get("code_id"):
        return False, "missing code_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("department_id"):
        return False, "missing department_id"
    if not d.get("code_type"):
        return False, "missing code_type"
    valid_severities = {"CRITICAL", "HIGH"}
    if d.get("severity") not in valid_severities:
        return False, f"invalid severity '{d.get('severity')}' — must be one of {valid_severities}"
    if not _parse_ts(d.get("ts", "")):
        return False, f"invalid ts '{d.get('ts')}'"
    return True, ""


# ── Converters (dict → typed Row) ─────────────────────────────────────────────

def convert_department(d: dict) -> Row:
    return Row(
        d["department_id"], d["department_name"], d.get("hospital_branch")
    )

def convert_vitals(d: dict) -> Row:
    return Row(
        d["event_id"], d["patient_id"],
        d.get("hospital"), d.get("ward"),
        int(d["heart_rate"]),
        float(d["spo2"]),
        int(d["systolic"]),
        int(d["diastolic"]),
        float(d["temperature_celsius"]),
        int(d["respiratory_rate"]),
        bool(d["is_anomaly"]),
        _parse_ts(d["ts"])
    )

def convert_lab(d: dict) -> Row:
    return Row(
        d["report_id"], d["patient_id"], d["doctor_id"],
        d.get("hospital"),
        d["test_name"],
        float(d["value"]),
        d.get("unit"), d.get("normal_range"),
        d["flag"],
        float(d["amount"]),
        _parse_ts(d["ts"])
    )

def convert_hospital_event(d: dict) -> Row:
    return Row(
        d["event_id"], d["patient_id"], d["department_id"],
        d.get("hospital"), d.get("ward"),
        d["event_type"],
        float(d["amount"]),
        _parse_ts(d["ts"])
    )

def convert_icu_code(d: dict) -> Row:
    return Row(
        d["code_id"], d["patient_id"], d["department_id"],
        d.get("hospital"), d.get("ward"),
        d["code_type"], d["severity"],
        float(d["amount"]),
        d["status"],
        _parse_ts(d["ts"])
    )


# ── ProcessFunction — validate + convert + route ──────────────────────────────

class ValidateAndConvert(ProcessFunction):
    def __init__(self, topic: str, validator, converter):
        self.topic     = topic
        self.validator = validator
        self.converter = converter

    def process_element(self, value, ctx: "ProcessFunction.Context"):
        try:
            data = json.loads(value)
        except json.JSONDecodeError as e:
            ctx.output(INVALID_TAG, json.dumps({
                "topic": self.topic, "error": f"JSON parse error: {e}", "raw": value
            }))
            return

        valid, error = self.validator(data)

        if valid:
            yield self.converter(data)
        else:
            ctx.output(INVALID_TAG, json.dumps({
                "topic": self.topic, "error": error,
                "record_id": data.get(list(data.keys())[0], "unknown")
            }))
            log.warning("[INVALID][%s] %s", self.topic, error)


# ── Builders ──────────────────────────────────────────────────────────────────

def build_kafka_source(topic: str) -> KafkaSource:
    return (
        KafkaSource.builder()
            .set_bootstrap_servers(KAFKA_BOOTSTRAP)
            .set_topics(topic)
            .set_group_id(f"flink-monitoring-{topic}")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
    )

def build_jdbc_sink(sql: str, type_info) -> JdbcSink:
    return JdbcSink.sink(
        sql,
        type_info,
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url(MYSQL_URL)
            .with_driver_name("com.mysql.cj.jdbc.Driver")
            .with_user_name(MYSQL_USER)
            .with_password(MYSQL_PASSWORD)
            .build(),
        JdbcExecutionOptions.builder()
            .with_batch_interval_ms(1000)
            .with_batch_size(100)
            .with_max_retries(3)
            .build()
    )


# ── Pipeline config — one entry per topic ─────────────────────────────────────

PIPELINES = [
    ("departments",     validate_department,    convert_department,    DEPARTMENT_TYPE,     DEPARTMENT_SQL),
    ("patient_vitals",  validate_vitals,        convert_vitals,        VITALS_TYPE,         VITALS_SQL),
    ("lab_reports",     validate_lab,           convert_lab,           LAB_TYPE,            LAB_SQL),
    ("hospital_events", validate_hospital_event, convert_hospital_event, HOSPITAL_EVENT_TYPE, HOSPITAL_EVENT_SQL),
    ("icu_codes",       validate_icu_code,      convert_icu_code,      ICU_CODE_TYPE,       ICU_CODE_SQL),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(CHECKPOINT_INTERVAL)

    for topic, validator, converter, type_info, sql in PIPELINES:
        raw = env.from_source(
            build_kafka_source(topic),
            WatermarkStrategy.no_watermarks(),
            f"kafka-source-{topic}"
        )

        processed = raw.process(
            ValidateAndConvert(topic, validator, converter),
            output_type=type_info
        )

        processed.add_sink(build_jdbc_sink(sql, type_info))

        processed.get_side_output(INVALID_TAG) \
                 .map(lambda x: f"[INVALID] {x}") \
                 .print()

        log.info("Pipeline registered: %s", topic)

    log.info("Starting Monitoring DataStream Pipeline...")
    env.execute("Monitoring DataStream Pipeline")


if __name__ == "__main__":
    main()
