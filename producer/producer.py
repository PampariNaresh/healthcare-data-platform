import json
import random
import time
import logging
import os
import uuid
from datetime import date, datetime, timedelta

from faker import Faker
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

fake = Faker()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SEND_INTERVAL     = float(os.getenv("SEND_INTERVAL", "2.0"))
MAX_EVENTS        = int(os.getenv("MAX_EVENTS", "0"))   # 0 = unlimited

TOPICS = [
    "patients", "doctors", "appointments", "treatments", "billing",
    "departments", "patient_vitals", "lab_reports", "hospital_events", "icu_codes",
]

# ── Existing constants ────────────────────────────────────────────────────────

SPECIALIZATIONS   = ["Cardiology", "Dermatology", "Pediatrics", "Orthopedics",
                     "Neurology", "Oncology", "Gastroenterology", "Psychiatry"]
HOSPITAL_BRANCHES = ["Central Hospital", "Westside Clinic", "Eastside Clinic",
                     "North Wing", "South Wing"]
INSURANCE_PROVIDERS = ["HealthIndia", "WellnessCorp", "PulseSecure", "MediShield", "CarePlus"]
REASONS           = ["Consultation", "Therapy", "Follow-up", "Emergency", "Check-up"]
STATUSES          = ["Scheduled", "Completed", "Cancelled", "No-show"]
TREATMENT_TYPES   = ["MRI", "CT Scan", "Chemotherapy", "Surgery",
                     "Physiotherapy", "X-Ray", "Blood Test", "ECG"]
DESCRIPTIONS      = ["Basic screening", "Advanced protocol", "Standard procedure",
                     "Emergency treatment", "Routine check"]
PAYMENT_METHODS   = ["Cash", "Insurance", "Card"]
PAYMENT_STATUSES  = ["Paid", "Pending", "Failed"]

# ── New constants ─────────────────────────────────────────────────────────────

DEPARTMENTS = [
    {"department_id": "DEPT01", "department_name": "ICU",        "hospital_branch": "Central Hospital"},
    {"department_id": "DEPT02", "department_name": "Cardiology", "hospital_branch": "Westside Clinic"},
    {"department_id": "DEPT03", "department_name": "Emergency",  "hospital_branch": "Central Hospital"},
    {"department_id": "DEPT04", "department_name": "Nephrology", "hospital_branch": "North Wing"},
    {"department_id": "DEPT05", "department_name": "Neurology",  "hospital_branch": "Eastside Clinic"},
]
DEPT_IDS = [d["department_id"] for d in DEPARTMENTS]

WARDS = ["Ward-A", "Ward-B", "ICU-1", "ICU-2", "ER-1"]

LAB_TESTS = [
    {"name": "Glucose",    "lo": 70,   "hi": 100,  "unit": "mg/dL",  "cost": 50.0},
    {"name": "Hemoglobin", "lo": 12,   "hi": 17.5, "unit": "g/dL",   "cost": 75.0},
    {"name": "WBC",        "lo": 4.5,  "hi": 11.0, "unit": "K/uL",   "cost": 80.0},
    {"name": "Creatinine", "lo": 0.6,  "hi": 1.2,  "unit": "mg/dL",  "cost": 90.0},
    {"name": "Troponin",   "lo": 0.0,  "hi": 0.04, "unit": "ng/mL",  "cost": 200.0},
    {"name": "Sodium",     "lo": 136,  "hi": 145,  "unit": "mEq/L",  "cost": 60.0},
]

HOSPITAL_EVENT_TYPES   = ["Admission", "Discharge", "Transfer",
                           "Emergency_Arrival", "Surgery_Start", "Surgery_End", "ICU_Transfer"]
HOSPITAL_EVENT_AMOUNTS = {
    "Admission": 1500.0, "Discharge": 250.0, "Transfer": 300.0,
    "Emergency_Arrival": 2500.0, "Surgery_Start": 8000.0,
    "Surgery_End": 0.0, "ICU_Transfer": 1200.0,
}

ICU_CODE_TYPES = ["Code_Blue", "STEMI_Alert", "Stroke_Alert",
                  "Rapid_Response", "Trauma_Activation"]
ICU_SEVERITY   = {
    "Code_Blue":         "CRITICAL",
    "STEMI_Alert":       "CRITICAL",
    "Stroke_Alert":      "CRITICAL",
    "Rapid_Response":    "HIGH",
    "Trauma_Activation": "CRITICAL",
}
ICU_AMOUNTS = {
    "Code_Blue":         5000.0,
    "STEMI_Alert":       12000.0,
    "Stroke_Alert":      10000.0,
    "Rapid_Response":    3000.0,
    "Trauma_Activation": 15000.0,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(start_year=2020, end_year=2024) -> str:
    start = date(start_year, 1, 1)
    delta = (date(end_year, 12, 31) - start).days
    return str(start + timedelta(days=random.randint(0, delta)))

def rand_time() -> str:
    return f"{random.randint(8, 17):02d}:{random.choice([0,15,30,45]):02d}:00"

def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


# ── Existing generators ───────────────────────────────────────────────────────

def gen_patient(pid: str) -> dict:
    fn, ln = fake.first_name(), fake.last_name()
    return {
        "patient_id":         pid,
        "first_name":         fn,
        "last_name":          ln,
        "gender":             random.choice(["M", "F"]),
        "date_of_birth":      rand_date(1950, 2000),
        "contact_number":     fake.numerify("##########"),
        "address":            fake.street_address(),
        "registration_date":  rand_date(2020, 2023),
        "insurance_provider": random.choice(INSURANCE_PROVIDERS),
        "insurance_number":   "INS" + fake.numerify("######"),
        "email":              f"{fn.lower()}.{ln.lower()}@mail.com",
    }

def gen_doctor(did: str) -> dict:
    fn, ln = fake.first_name(), fake.last_name()
    return {
        "doctor_id":        did,
        "first_name":       fn,
        "last_name":        ln,
        "specialization":   random.choice(SPECIALIZATIONS),
        "phone_number":     fake.numerify("##########"),
        "years_experience": random.randint(1, 35),
        "hospital_branch":  random.choice(HOSPITAL_BRANCHES),
        "email":            f"dr.{fn.lower()}.{ln.lower()}@hospital.com",
    }

def gen_appointment(aid: str, pid: str, did: str) -> dict:
    appt_date = rand_date(2023, 2024)
    return {
        "appointment_id":   aid,
        "patient_id":       pid,
        "doctor_id":        did,
        "appointment_date": appt_date,
        "appointment_time": rand_time(),
        "reason_for_visit": random.choice(REASONS),
        "status":           random.choice(STATUSES),
    }

def gen_treatment(tid: str, aid: str, appt_date: str) -> dict:
    return {
        "treatment_id":    tid,
        "appointment_id":  aid,
        "treatment_type":  random.choice(TREATMENT_TYPES),
        "description":     random.choice(DESCRIPTIONS),
        "cost":            round(random.uniform(500.0, 8000.0), 2),
        "treatment_date":  appt_date,
    }

def gen_billing(bid: str, pid: str, tid: str, bill_date: str, amount: float) -> dict:
    return {
        "bill_id":         bid,
        "patient_id":      pid,
        "treatment_id":    tid,
        "bill_date":       bill_date,
        "amount":          amount,
        "payment_method":  random.choice(PAYMENT_METHODS),
        "payment_status":  random.choice(PAYMENT_STATUSES),
    }


# ── New generators ────────────────────────────────────────────────────────────

def gen_department(dept: dict) -> dict:
    return {
        "department_id":   dept["department_id"],
        "department_name": dept["department_name"],
        "hospital_branch": dept["hospital_branch"],
    }

def gen_patient_vitals(pid: str) -> dict:
    anomaly = random.random() < 0.15
    return {
        "event_id":            str(uuid.uuid4()),
        "patient_id":          pid,
        "hospital":            "City General Hospital",
        "ward":                random.choice(WARDS),
        "heart_rate":          random.randint(140, 180) if anomaly else random.randint(60, 100),
        "spo2":                round(random.uniform(85.0, 92.0), 1) if anomaly else round(random.uniform(95.0, 100.0), 1),
        "systolic":            random.randint(140, 190) if anomaly else random.randint(90, 120),
        "diastolic":           random.randint(90, 120)  if anomaly else random.randint(60, 80),
        "temperature_celsius": round(random.uniform(38.5, 40.5), 1) if anomaly else round(random.uniform(36.1, 37.5), 1),
        "respiratory_rate":    random.randint(22, 30) if anomaly else random.randint(12, 20),
        "is_anomaly":          anomaly,
        "ts":                  now_ts(),
    }

def gen_lab_report(pid: str, did: str) -> dict:
    test  = random.choice(LAB_TESTS)
    value = round(random.uniform(test["lo"] * 0.5, test["hi"] * 1.5), 3)
    if value < test["lo"] * 0.7 or value > test["hi"] * 3:
        flag = "critical"
    elif value < test["lo"]:
        flag = "low"
    elif value > test["hi"]:
        flag = "high"
    else:
        flag = "normal"
    return {
        "report_id":    str(uuid.uuid4()),
        "patient_id":   pid,
        "doctor_id":    did,
        "hospital":     "City General Hospital",
        "test_name":    test["name"],
        "value":        value,
        "unit":         test["unit"],
        "normal_range": f"{test['lo']}-{test['hi']}",
        "flag":         flag,
        "amount":       round(test["cost"] * 1.5 if flag == "critical" else test["cost"], 2),
        "ts":           now_ts(),
    }

def gen_hospital_event(pid: str, dept_id: str) -> dict:
    etype = random.choice(HOSPITAL_EVENT_TYPES)
    return {
        "event_id":      str(uuid.uuid4()),
        "patient_id":    pid,
        "department_id": dept_id,
        "hospital":      "City General Hospital",
        "ward":          random.choice(WARDS),
        "event_type":    etype,
        "amount":        HOSPITAL_EVENT_AMOUNTS.get(etype, 500.0),
        "ts":            now_ts(),
    }

def gen_icu_code(pid: str, dept_id: str) -> dict:
    code = random.choice(ICU_CODE_TYPES)
    return {
        "code_id":       str(uuid.uuid4()),
        "patient_id":    pid,
        "department_id": dept_id,
        "hospital":      "City General Hospital",
        "ward":          random.choice(WARDS),
        "code_type":     code,
        "severity":      ICU_SEVERITY[code],
        "amount":        ICU_AMOUNTS.get(code, 1000.0),
        "status":        "Activated",
        "ts":            now_ts(),
    }


# ── Kafka setup ───────────────────────────────────────────────────────────────

def wait_for_kafka(retries=15, delay=5):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVERS])
            p.close()
            log.info("Kafka is ready.")
            return
        except NoBrokersAvailable:
            log.info("Waiting for Kafka... (%d/%d)", i + 1, retries)
            time.sleep(delay)
    raise RuntimeError("Kafka not available after retries")

def ensure_topics():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    existing = set(admin.list_topics())
    missing  = [t for t in TOPICS if t not in existing]

    if missing:
        new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in missing]
        try:
            admin.create_topics(new_topics)
            log.info("Created topics: %s", missing)
        except TopicAlreadyExistsError:
            pass
    else:
        log.info("All topics already exist.")

    admin.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    wait_for_kafka()
    ensure_topics()

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    # ── Phase 1 — initial seed (patients, doctors, departments) ──────────────
    patient_ids = [f"P{i:03d}" for i in range(1, 21)]
    doctor_ids  = [f"D{i:03d}" for i in range(1, 11)]

    log.info("Sending initial patients...")
    for pid in patient_ids:
        producer.send("patients", key=pid, value=gen_patient(pid))
        time.sleep(0.05)
    producer.flush()
    log.info("  Sent %d patients", len(patient_ids))

    time.sleep(1)

    log.info("Sending initial doctors...")
    for did in doctor_ids:
        producer.send("doctors", key=did, value=gen_doctor(did))
        time.sleep(0.05)
    producer.flush()
    log.info("  Sent %d doctors", len(doctor_ids))

    time.sleep(1)

    log.info("Sending initial departments...")
    for dept in DEPARTMENTS:
        producer.send("departments", key=dept["department_id"], value=gen_department(dept))
        time.sleep(0.05)
    producer.flush()
    log.info("  Sent %d departments", len(DEPARTMENTS))

    time.sleep(1)

    # ── Event loop ────────────────────────────────────────────────────────────
    counter = 1
    limit_msg = f"limit={MAX_EVENTS}" if MAX_EVENTS > 0 else "unlimited"
    log.info("Generating events every %.1f seconds... (%s)", SEND_INTERVAL, limit_msg)

    while True:
        if MAX_EVENTS > 0 and counter > MAX_EVENTS:
            log.info("Reached MAX_EVENTS=%d. Producer done.", MAX_EVENTS)
            break

        pid     = random.choice(patient_ids)
        did     = random.choice(doctor_ids)
        dept_id = random.choice(DEPT_IDS)

        aid = f"A{counter:04d}"
        tid = f"T{counter:04d}"
        bid = f"B{counter:04d}"

        # existing events
        appt  = gen_appointment(aid, pid, did)
        treat = gen_treatment(tid, aid, appt["appointment_date"])
        bill  = gen_billing(bid, pid, tid, treat["treatment_date"], treat["cost"])

        producer.send("appointments", key=aid,          value=appt)
        producer.send("treatments",   key=tid,          value=treat)
        producer.send("billing",      key=bid,          value=bill)

        # new monitoring events
        vitals = gen_patient_vitals(pid)
        lab    = gen_lab_report(pid, did)
        hevt   = gen_hospital_event(pid, dept_id)
        icu    = gen_icu_code(pid, random.choice(DEPT_IDS))

        producer.send("patient_vitals",  key=vitals["event_id"], value=vitals)
        producer.send("lab_reports",     key=lab["report_id"],   value=lab)
        producer.send("hospital_events", key=hevt["event_id"],   value=hevt)
        producer.send("icu_codes",       key=icu["code_id"],     value=icu)

        producer.flush()

        anomaly_flag = " [ANOMALY]" if vitals["is_anomaly"] else ""
        log.info(
            "Event #%04d → appt=%s pid=%s did=%s dept=%s | "
            "vitals%s lab=%s(%s) hevt=%s icu=%s",
            counter, aid, pid, did, dept_id,
            anomaly_flag, lab["test_name"], lab["flag"],
            hevt["event_type"], icu["code_type"],
        )

        counter += 1
        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
