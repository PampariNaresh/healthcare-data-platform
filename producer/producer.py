import json
import random
import time
import logging
import os
from datetime import date, timedelta

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

TOPICS = ["patients", "doctors", "appointments", "treatments", "billing"]

SPECIALIZATIONS  = ["Cardiology", "Dermatology", "Pediatrics", "Orthopedics",
                    "Neurology", "Oncology", "Gastroenterology", "Psychiatry"]
HOSPITAL_BRANCHES = ["Central Hospital", "Westside Clinic", "Eastside Clinic",
                     "North Wing", "South Wing"]
INSURANCE_PROVIDERS = ["HealthIndia", "WellnessCorp", "PulseSecure", "MediShield", "CarePlus"]
REASONS          = ["Consultation", "Therapy", "Follow-up", "Emergency", "Check-up"]
STATUSES         = ["Scheduled", "Completed", "Cancelled", "No-show"]
TREATMENT_TYPES  = ["MRI", "CT Scan", "Chemotherapy", "Surgery",
                    "Physiotherapy", "X-Ray", "Blood Test", "ECG"]
DESCRIPTIONS     = ["Basic screening", "Advanced protocol", "Standard procedure",
                    "Emergency treatment", "Routine check"]
PAYMENT_METHODS  = ["Cash", "Insurance", "Card"]
PAYMENT_STATUSES = ["Paid", "Pending", "Failed"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(start_year=2020, end_year=2024) -> str:
    start = date(start_year, 1, 1)
    delta = (date(end_year, 12, 31) - start).days
    return str(start + timedelta(days=random.randint(0, delta)))

def rand_time() -> str:
    return f"{random.randint(8, 17):02d}:{random.choice([0,15,30,45]):02d}:00"


# ── Generators ────────────────────────────────────────────────────────────────

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

    # ── Initial batch: 20 patients + 10 doctors ───────────────────────────────
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

    # ── Continuous event generation ───────────────────────────────────────────
    counter = 1
    limit_msg = f"limit={MAX_EVENTS}" if MAX_EVENTS > 0 else "unlimited"
    log.info("Generating events every %.1f seconds... (%s)", SEND_INTERVAL, limit_msg)

    while True:
        if MAX_EVENTS > 0 and counter > MAX_EVENTS:
            log.info("Reached MAX_EVENTS=%d. Producer done.", MAX_EVENTS)
            break
        aid = f"A{counter:04d}"
        tid = f"T{counter:04d}"
        bid = f"B{counter:04d}"
        pid = random.choice(patient_ids)
        did = random.choice(doctor_ids)

        appt  = gen_appointment(aid, pid, did)
        treat = gen_treatment(tid, aid, appt["appointment_date"])
        bill  = gen_billing(bid, pid, tid, treat["treatment_date"], treat["cost"])

        producer.send("appointments", key=aid, value=appt)
        producer.send("treatments",   key=tid, value=treat)
        producer.send("billing",      key=bid, value=bill)
        producer.flush()

        log.info("Event #%04d → appt=%s | patient=%s | doctor=%s | cost=%.2f",
                 counter, aid, pid, did, treat["cost"])

        counter += 1
        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()
