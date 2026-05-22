import json
import uuid
from datetime import date
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config

router = APIRouter()


def _producer():
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        request_timeout_ms=5000,
    )


def _send(topic: str, key: str, payload: dict):
    try:
        prod = _producer()
        future = prod.send(topic, key=key.encode(), value=payload)
        record = future.get(timeout=10)
        prod.flush()
        prod.close()
        return {"success": True, "topic": topic, "partition": record.partition, "offset": record.offset}
    except KafkaError as e:
        raise HTTPException(status_code=502, detail=f"Kafka error: {e}")


# ── Patient ───────────────────────────────────────────────────────────────────
class PatientIn(BaseModel):
    first_name: str
    last_name: str
    gender: str
    date_of_birth: date
    contact_number: str
    address: Optional[str] = None
    insurance_provider: Optional[str] = None
    insurance_number: Optional[str] = None
    email: Optional[str] = None


@router.post("/patient")
def register_patient(body: PatientIn):
    patient_id = "P-" + uuid.uuid4().hex[:6].upper()
    payload = {"patient_id": patient_id, "registration_date": str(date.today()), **body.model_dump()}
    return _send("patients", patient_id, payload)


# ── Doctor ────────────────────────────────────────────────────────────────────
class DoctorIn(BaseModel):
    first_name: str
    last_name: str
    specialization: str
    phone_number: str
    years_experience: int
    hospital_branch: Optional[str] = None
    email: Optional[str] = None


@router.post("/doctor")
def add_doctor(body: DoctorIn):
    doctor_id = "D-" + uuid.uuid4().hex[:6].upper()
    payload = {"doctor_id": doctor_id, **body.model_dump()}
    return _send("doctors", doctor_id, payload)


# ── Appointment ───────────────────────────────────────────────────────────────
class AppointmentIn(BaseModel):
    patient_id: str
    doctor_id: str
    appointment_date: date
    appointment_time: str
    reason_for_visit: Optional[str] = None
    status: str = "Scheduled"


@router.post("/appointment")
def schedule_appointment(body: AppointmentIn):
    appt_id = "A-" + uuid.uuid4().hex[:6].upper()
    payload = {"appointment_id": appt_id, **body.model_dump()}
    return _send("appointments", appt_id, payload)


# ── Treatment ─────────────────────────────────────────────────────────────────
class TreatmentIn(BaseModel):
    appointment_id: str
    treatment_type: str
    description: Optional[str] = None
    cost: float
    treatment_date: date


@router.post("/treatment")
def record_treatment(body: TreatmentIn):
    treatment_id = "T-" + uuid.uuid4().hex[:6].upper()
    payload = {"treatment_id": treatment_id, **body.model_dump()}
    return _send("treatments", treatment_id, payload)


# ── Billing ───────────────────────────────────────────────────────────────────
class BillingIn(BaseModel):
    patient_id: str
    treatment_id: str
    bill_date: date
    amount: float
    payment_method: Optional[str] = None
    payment_status: str = "Pending"


@router.post("/billing")
def generate_bill(body: BillingIn):
    bill_id = "B-" + uuid.uuid4().hex[:6].upper()
    payload = {"bill_id": bill_id, **body.model_dump()}
    return _send("billing", bill_id, payload)
