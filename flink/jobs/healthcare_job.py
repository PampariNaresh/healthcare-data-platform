
"""
Healthcare DataStream Pipeline
  Kafka topics → validate → MySQL tables
  Invalid records go to side output (printed to Flink logs)
"""
import json
import logging
import re
from datetime import datetime, date, time as dt_time
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

# ── Config (from environment / .env via docker-compose) ───────────────────────

KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MYSQL_URL            = os.getenv("MYSQL_URL",
                           "jdbc:mysql://mysql:3306/healthcare"
                           "?useSSL=false&allowPublicKeyRetrieval=true"
                           "&serverTimezone=UTC&sessionVariables=foreign_key_checks=0")
MYSQL_USER           = os.getenv("MYSQL_USER",     "hc_user")
MYSQL_PASSWORD       = os.getenv("MYSQL_PASSWORD", "hc_pass")
CHECKPOINT_INTERVAL  = int(os.getenv("FLINK_CHECKPOINT_INTERVAL", "10000"))

INVALID_TAG = OutputTag("invalid-records", Types.STRING())


# ── Type Infos ────────────────────────────────────────────────────────────────

PATIENT_TYPE = Types.ROW_NAMED(
    ["patient_id", "first_name", "last_name", "gender", "date_of_birth",
     "contact_number", "address", "registration_date", "insurance_provider",
     "insurance_number", "email"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_DATE(),
     Types.STRING(), Types.STRING(), Types.SQL_DATE(), Types.STRING(),
     Types.STRING(), Types.STRING()]
)

DOCTOR_TYPE = Types.ROW_NAMED(
    ["doctor_id", "first_name", "last_name", "specialization", "phone_number",
     "years_experience", "hospital_branch", "email"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.INT(), Types.STRING(), Types.STRING()]
)

APPOINTMENT_TYPE = Types.ROW_NAMED(
    ["appointment_id", "patient_id", "doctor_id", "appointment_date",
     "appointment_time", "reason_for_visit", "status"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_DATE(),
     Types.SQL_TIME(), Types.STRING(), Types.STRING()]
)

TREATMENT_TYPE = Types.ROW_NAMED(
    ["treatment_id", "appointment_id", "treatment_type", "description",
     "cost", "treatment_date"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
     Types.DOUBLE(), Types.SQL_DATE()]
)

BILLING_TYPE = Types.ROW_NAMED(
    ["bill_id", "patient_id", "treatment_id", "bill_date", "amount",
     "payment_method", "payment_status"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.SQL_DATE(),
     Types.DOUBLE(), Types.STRING(), Types.STRING()]
)


# ── SQL (upsert via ON DUPLICATE KEY UPDATE) ──────────────────────────────────

PATIENT_SQL = """
    INSERT INTO patients
        (patient_id, first_name, last_name, gender, date_of_birth,
         contact_number, address, registration_date, insurance_provider,
         insurance_number, email)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        first_name=VALUES(first_name), last_name=VALUES(last_name),
        gender=VALUES(gender), contact_number=VALUES(contact_number),
        address=VALUES(address), insurance_provider=VALUES(insurance_provider),
        insurance_number=VALUES(insurance_number), email=VALUES(email)
"""

DOCTOR_SQL = """
    INSERT INTO doctors
        (doctor_id, first_name, last_name, specialization, phone_number,
         years_experience, hospital_branch, email)
    VALUES (?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        first_name=VALUES(first_name), last_name=VALUES(last_name),
        specialization=VALUES(specialization), phone_number=VALUES(phone_number),
        years_experience=VALUES(years_experience),
        hospital_branch=VALUES(hospital_branch), email=VALUES(email)
"""

APPOINTMENT_SQL = """
    INSERT INTO appointments
        (appointment_id, patient_id, doctor_id, appointment_date,
         appointment_time, reason_for_visit, status)
    VALUES (?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        appointment_date=VALUES(appointment_date),
        appointment_time=VALUES(appointment_time),
        reason_for_visit=VALUES(reason_for_visit),
        status=VALUES(status)
"""

TREATMENT_SQL = """
    INSERT INTO treatments
        (treatment_id, appointment_id, treatment_type, description,
         cost, treatment_date)
    VALUES (?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        treatment_type=VALUES(treatment_type), description=VALUES(description),
        cost=VALUES(cost), treatment_date=VALUES(treatment_date)
"""

BILLING_SQL = """
    INSERT INTO billing
        (bill_id, patient_id, treatment_id, bill_date, amount,
         payment_method, payment_status)
    VALUES (?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE
        bill_date=VALUES(bill_date), amount=VALUES(amount),
        payment_method=VALUES(payment_method),
        payment_status=VALUES(payment_status)
"""


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _parse_time(s: str) -> Optional[dt_time]:
    try:
        return datetime.strptime(s, "%H:%M:%S").time()
    except Exception:
        return None

def _valid_email(s: str) -> bool:
    if not s:
        return True
    return bool(re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", s))


# ── Validators ────────────────────────────────────────────────────────────────

def validate_patient(d: dict) -> Tuple[bool, str]:
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("first_name"):
        return False, "missing first_name"
    if not d.get("last_name"):
        return False, "missing last_name"
    if d.get("gender") not in ("M", "F"):
        return False, f"invalid gender '{d.get('gender')}' — must be M or F"
    if not _parse_date(d.get("date_of_birth", "")):
        return False, f"invalid date_of_birth '{d.get('date_of_birth')}'"
    if not _parse_date(d.get("registration_date", "")):
        return False, f"invalid registration_date '{d.get('registration_date')}'"
    if not _valid_email(d.get("email", "")):
        return False, f"invalid email '{d.get('email')}'"
    return True, ""

def validate_doctor(d: dict) -> Tuple[bool, str]:
    if not d.get("doctor_id"):
        return False, "missing doctor_id"
    if not d.get("first_name"):
        return False, "missing first_name"
    if not d.get("last_name"):
        return False, "missing last_name"
    if not d.get("specialization"):
        return False, "missing specialization"
    if not d.get("phone_number"):
        return False, "missing phone_number"
    try:
        exp = int(d.get("years_experience", -1))
        if not (0 <= exp <= 60):
            return False, f"years_experience out of range: {exp}"
    except (ValueError, TypeError):
        return False, f"invalid years_experience '{d.get('years_experience')}'"
    return True, ""

def validate_appointment(d: dict) -> Tuple[bool, str]:
    if not d.get("appointment_id"):
        return False, "missing appointment_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("doctor_id"):
        return False, "missing doctor_id"
    if not _parse_date(d.get("appointment_date", "")):
        return False, f"invalid appointment_date '{d.get('appointment_date')}'"
    if not _parse_time(d.get("appointment_time", "")):
        return False, f"invalid appointment_time '{d.get('appointment_time')}'"
    valid_statuses = {"Scheduled", "Completed", "Cancelled", "No-show"}
    if d.get("status") not in valid_statuses:
        return False, f"invalid status '{d.get('status')}' — must be one of {valid_statuses}"
    return True, ""

def validate_treatment(d: dict) -> Tuple[bool, str]:
    if not d.get("treatment_id"):
        return False, "missing treatment_id"
    if not d.get("appointment_id"):
        return False, "missing appointment_id"
    if not d.get("treatment_type"):
        return False, "missing treatment_type"
    if not _parse_date(d.get("treatment_date", "")):
        return False, f"invalid treatment_date '{d.get('treatment_date')}'"
    try:
        cost = float(d.get("cost", 0))
        if cost <= 0:
            return False, f"cost must be > 0, got {cost}"
    except (ValueError, TypeError):
        return False, f"invalid cost '{d.get('cost')}'"
    return True, ""

def validate_billing(d: dict) -> Tuple[bool, str]:
    if not d.get("bill_id"):
        return False, "missing bill_id"
    if not d.get("patient_id"):
        return False, "missing patient_id"
    if not d.get("treatment_id"):
        return False, "missing treatment_id"
    if not _parse_date(d.get("bill_date", "")):
        return False, f"invalid bill_date '{d.get('bill_date')}'"
    try:
        amount = float(d.get("amount", 0))
        if amount <= 0:
            return False, f"amount must be > 0, got {amount}"
    except (ValueError, TypeError):
        return False, f"invalid amount '{d.get('amount')}'"
    valid_methods = {"Cash", "Insurance", "Card"}
    if d.get("payment_method") not in valid_methods:
        return False, f"invalid payment_method '{d.get('payment_method')}'"
    valid_statuses = {"Paid", "Pending", "Failed"}
    if d.get("payment_status") not in valid_statuses:
        return False, f"invalid payment_status '{d.get('payment_status')}'"
    return True, ""


# ── Converters (dict → typed Row) ─────────────────────────────────────────────

def convert_patient(d: dict) -> Row:
    return Row(
        d["patient_id"], d["first_name"], d["last_name"], d["gender"],
        _parse_date(d["date_of_birth"]),
        d["contact_number"], d.get("address"),
        _parse_date(d["registration_date"]),
        d.get("insurance_provider"), d.get("insurance_number"), d.get("email")
    )

def convert_doctor(d: dict) -> Row:
    return Row(
        d["doctor_id"], d["first_name"], d["last_name"],
        d["specialization"], d["phone_number"],
        int(d["years_experience"]),
        d.get("hospital_branch"), d.get("email")
    )

def convert_appointment(d: dict) -> Row:
    return Row(
        d["appointment_id"], d["patient_id"], d["doctor_id"],
        _parse_date(d["appointment_date"]),
        _parse_time(d["appointment_time"]),
        d.get("reason_for_visit"), d["status"]
    )

def convert_treatment(d: dict) -> Row:
    return Row(
        d["treatment_id"], d["appointment_id"],
        d["treatment_type"], d.get("description"),
        float(d["cost"]), _parse_date(d["treatment_date"])
    )

def convert_billing(d: dict) -> Row:
    return Row(
        d["bill_id"], d["patient_id"], d["treatment_id"],
        _parse_date(d["bill_date"]),
        float(d["amount"]),
        d.get("payment_method"), d["payment_status"]
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
            .set_group_id(f"flink-healthcare-{topic}")
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
    ("patients",     validate_patient,     convert_patient,     PATIENT_TYPE,     PATIENT_SQL),
    ("doctors",      validate_doctor,      convert_doctor,      DOCTOR_TYPE,      DOCTOR_SQL),
    ("appointments", validate_appointment, convert_appointment, APPOINTMENT_TYPE, APPOINTMENT_SQL),
    ("treatments",   validate_treatment,   convert_treatment,   TREATMENT_TYPE,   TREATMENT_SQL),
    ("billing",      validate_billing,     convert_billing,     BILLING_TYPE,     BILLING_SQL),
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

        # valid records → MySQL
        processed.add_sink(build_jdbc_sink(sql, type_info))

        # invalid records → Flink task logs
        processed.get_side_output(INVALID_TAG) \
                 .map(lambda x: f"[INVALID] {x}") \
                 .print()

        log.info("Pipeline registered: %s", topic)

    log.info("Starting Healthcare DataStream Pipeline...")
    env.execute("Healthcare DataStream Pipeline")


if __name__ == "__main__":
    main()
