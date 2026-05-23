import json
import re
import uuid
from datetime import date, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, field_validator
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config

router = APIRouter()

# ── Constants ─────────────────────────────────────────────────────────────────

VALID_SPECIALIZATIONS = {
    "Cardiology", "Dermatology", "Pediatrics", "Orthopedics",
    "Neurology", "Oncology", "Gastroenterology", "Psychiatry",
    "General Medicine", "Gynecology", "Radiology", "Urology",
}
VALID_TREATMENT_TYPES = {
    "MRI", "CT Scan", "Chemotherapy", "Surgery", "Physiotherapy",
    "X-Ray", "Blood Test", "ECG", "Consultation", "Vaccination",
    "Dental", "Emergency",
}
VALID_PAYMENT_METHODS  = {"Cash", "Insurance", "Card", "Credit Card", "Debit Card", "UPI", "Net Banking"}
VALID_PAYMENT_STATUSES = {"Paid", "Pending", "Failed", "Refunded"}
VALID_APPT_STATUSES    = {"Scheduled", "Completed", "Cancelled", "No-show"}


# ── Kafka helpers ─────────────────────────────────────────────────────────────

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

    @field_validator("first_name", "last_name")
    @classmethod
    def name_length(cls, v):
        v = v.strip()
        if len(v) < 2:
            raise ValueError("must be at least 2 characters")
        return v

    @field_validator("gender")
    @classmethod
    def valid_gender(cls, v):
        if v not in ("M", "F"):
            raise ValueError("must be M or F")
        return v

    @field_validator("contact_number")
    @classmethod
    def valid_phone(cls, v):
        digits = re.sub(r"\D", "", v)
        if len(digits) != 10:
            raise ValueError("must be exactly 10 digits")
        return digits

    @field_validator("date_of_birth")
    @classmethod
    def valid_dob(cls, v):
        today = date.today()
        if v >= today:
            raise ValueError("must be in the past")
        age = (today - v).days / 365.25
        if age > 120:
            raise ValueError("invalid date of birth")
        return v

    @field_validator("email")
    @classmethod
    def valid_email(cls, v):
        if v and not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", v):
            raise ValueError("invalid email format")
        return v


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

    @field_validator("first_name", "last_name")
    @classmethod
    def name_length(cls, v):
        v = v.strip()
        if len(v) < 2:
            raise ValueError("must be at least 2 characters")
        return v

    @field_validator("specialization")
    @classmethod
    def valid_specialization(cls, v):
        if v not in VALID_SPECIALIZATIONS:
            raise ValueError(f"must be one of {sorted(VALID_SPECIALIZATIONS)}")
        return v

    @field_validator("phone_number")
    @classmethod
    def valid_phone(cls, v):
        digits = re.sub(r"\D", "", v)
        if len(digits) != 10:
            raise ValueError("must be exactly 10 digits")
        return digits

    @field_validator("years_experience")
    @classmethod
    def valid_experience(cls, v):
        if not (0 <= v <= 60):
            raise ValueError("must be between 0 and 60")
        return v

    @field_validator("email")
    @classmethod
    def valid_email(cls, v):
        if v and not re.match(r"^[^\s@]+@[^\s@]+\.[^\s@]+$", v):
            raise ValueError("invalid email format")
        return v


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

    @field_validator("patient_id", "doctor_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("appointment_time")
    @classmethod
    def valid_time(cls, v):
        # Accept HH:MM or HH:MM:SS
        if re.match(r"^\d{2}:\d{2}$", v):
            return v + ":00"
        if re.match(r"^\d{2}:\d{2}:\d{2}$", v):
            return v
        raise ValueError("must be in HH:MM format")

    @field_validator("status")
    @classmethod
    def valid_status(cls, v):
        if v not in VALID_APPT_STATUSES:
            raise ValueError(f"must be one of {sorted(VALID_APPT_STATUSES)}")
        return v


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

    @field_validator("appointment_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("treatment_type")
    @classmethod
    def valid_type(cls, v):
        if v not in VALID_TREATMENT_TYPES:
            raise ValueError(f"must be one of {sorted(VALID_TREATMENT_TYPES)}")
        return v

    @field_validator("cost")
    @classmethod
    def valid_cost(cls, v):
        if v <= 0:
            raise ValueError("must be greater than 0")
        return round(v, 2)


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

    @field_validator("patient_id", "treatment_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("amount")
    @classmethod
    def valid_amount(cls, v):
        if v <= 0:
            raise ValueError("must be greater than 0")
        return round(v, 2)

    @field_validator("payment_method")
    @classmethod
    def valid_method(cls, v):
        if v and v not in VALID_PAYMENT_METHODS:
            raise ValueError(f"must be one of {sorted(VALID_PAYMENT_METHODS)}")
        return v

    @field_validator("payment_status")
    @classmethod
    def valid_status(cls, v):
        if v not in VALID_PAYMENT_STATUSES:
            raise ValueError(f"must be one of {sorted(VALID_PAYMENT_STATUSES)}")
        return v


@router.post("/billing")
def generate_bill(body: BillingIn):
    bill_id = "B-" + uuid.uuid4().hex[:6].upper()
    payload = {"bill_id": bill_id, **body.model_dump()}
    return _send("billing", bill_id, payload)


# ── Department ────────────────────────────────────────────────────────────────

VALID_DEPT_BRANCHES = {
    "Central Hospital", "Westside Clinic", "Eastside Clinic", "North Wing", "South Wing"
}

class DepartmentIn(BaseModel):
    department_id: str
    department_name: str
    hospital_branch: Optional[str] = None

    @field_validator("department_id")
    @classmethod
    def valid_dept_id(cls, v):
        v = v.strip().upper()
        if len(v) < 2:
            raise ValueError("must be at least 2 characters")
        return v

    @field_validator("department_name")
    @classmethod
    def valid_dept_name(cls, v):
        v = v.strip()
        if len(v) < 2:
            raise ValueError("must be at least 2 characters")
        return v

    @field_validator("hospital_branch")
    @classmethod
    def valid_branch(cls, v):
        if v and v not in VALID_DEPT_BRANCHES:
            raise ValueError(f"must be one of {sorted(VALID_DEPT_BRANCHES)}")
        return v


@router.post("/department")
def add_department(body: DepartmentIn):
    payload = body.model_dump()
    return _send("departments", payload["department_id"], payload)


# ── Patient Vitals ────────────────────────────────────────────────────────────

VALID_WARDS = {"Ward-A", "Ward-B", "ICU-1", "ICU-2", "ER-1"}

class PatientVitalsIn(BaseModel):
    patient_id: str
    hospital: Optional[str] = None
    ward: Optional[str] = None
    heart_rate: int
    spo2: float
    systolic: int
    diastolic: int
    temperature_celsius: float
    respiratory_rate: int
    is_anomaly: bool = False
    ts: Optional[datetime] = None

    @field_validator("patient_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("heart_rate")
    @classmethod
    def valid_hr(cls, v):
        if not (30 <= v <= 250):
            raise ValueError("must be between 30 and 250")
        return v

    @field_validator("spo2")
    @classmethod
    def valid_spo2(cls, v):
        if not (50.0 <= v <= 100.0):
            raise ValueError("must be between 50.0 and 100.0")
        return round(v, 1)

    @field_validator("systolic")
    @classmethod
    def valid_systolic(cls, v):
        if not (50 <= v <= 250):
            raise ValueError("must be between 50 and 250")
        return v

    @field_validator("diastolic")
    @classmethod
    def valid_diastolic(cls, v):
        if not (30 <= v <= 150):
            raise ValueError("must be between 30 and 150")
        return v

    @field_validator("temperature_celsius")
    @classmethod
    def valid_temp(cls, v):
        if not (33.0 <= v <= 45.0):
            raise ValueError("must be between 33.0 and 45.0")
        return round(v, 1)

    @field_validator("respiratory_rate")
    @classmethod
    def valid_rr(cls, v):
        if not (5 <= v <= 60):
            raise ValueError("must be between 5 and 60")
        return v


@router.post("/patient-vitals")
def record_vitals(body: PatientVitalsIn):
    event_id = str(uuid.uuid4())
    ts = body.ts or datetime.utcnow()
    data = body.model_dump()
    data["event_id"] = event_id
    data["ts"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
    return _send("patient_vitals", event_id, data)


# ── Lab Report ────────────────────────────────────────────────────────────────

VALID_LAB_TESTS = {"Glucose", "Hemoglobin", "WBC", "Creatinine", "Troponin", "Sodium"}
VALID_FLAGS     = {"normal", "low", "high", "critical"}

class LabReportIn(BaseModel):
    patient_id: str
    doctor_id: str
    hospital: Optional[str] = None
    test_name: str
    value: float
    unit: Optional[str] = None
    normal_range: Optional[str] = None
    flag: str
    amount: float
    ts: Optional[datetime] = None

    @field_validator("patient_id", "doctor_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("test_name")
    @classmethod
    def valid_test(cls, v):
        if v not in VALID_LAB_TESTS:
            raise ValueError(f"must be one of {sorted(VALID_LAB_TESTS)}")
        return v

    @field_validator("value")
    @classmethod
    def valid_value(cls, v):
        if v < 0:
            raise ValueError("must be non-negative")
        return round(v, 3)

    @field_validator("flag")
    @classmethod
    def valid_flag(cls, v):
        if v not in VALID_FLAGS:
            raise ValueError(f"must be one of {sorted(VALID_FLAGS)}")
        return v

    @field_validator("amount")
    @classmethod
    def valid_amount(cls, v):
        if v < 0:
            raise ValueError("must be non-negative")
        return round(v, 2)


@router.post("/lab-report")
def record_lab_report(body: LabReportIn):
    report_id = str(uuid.uuid4())
    ts = body.ts or datetime.utcnow()
    data = body.model_dump()
    data["report_id"] = report_id
    data["ts"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
    return _send("lab_reports", report_id, data)


# ── Hospital Event ────────────────────────────────────────────────────────────

VALID_EVENT_TYPES = {
    "Admission", "Discharge", "Transfer",
    "Emergency_Arrival", "Surgery_Start", "Surgery_End", "ICU_Transfer",
}

class HospitalEventIn(BaseModel):
    patient_id: str
    department_id: str
    hospital: Optional[str] = None
    ward: Optional[str] = None
    event_type: str
    amount: float = 0.0
    ts: Optional[datetime] = None

    @field_validator("patient_id", "department_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("event_type")
    @classmethod
    def valid_event_type(cls, v):
        if v not in VALID_EVENT_TYPES:
            raise ValueError(f"must be one of {sorted(VALID_EVENT_TYPES)}")
        return v

    @field_validator("amount")
    @classmethod
    def valid_amount(cls, v):
        if v < 0:
            raise ValueError("must be non-negative")
        return round(v, 2)


@router.post("/hospital-event")
def record_hospital_event(body: HospitalEventIn):
    event_id = str(uuid.uuid4())
    ts = body.ts or datetime.utcnow()
    data = body.model_dump()
    data["event_id"] = event_id
    data["ts"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
    return _send("hospital_events", event_id, data)


# ── ICU Code ──────────────────────────────────────────────────────────────────

VALID_ICU_TYPES     = {"Code_Blue", "Rapid_Response", "STEMI_Alert", "Stroke_Alert", "Trauma_Activation"}
VALID_ICU_SEVERITY  = {"CRITICAL", "HIGH"}
VALID_ICU_STATUSES  = {"Activated", "Resolved", "False_Alarm"}

class IcuCodeIn(BaseModel):
    patient_id: str
    department_id: str
    hospital: Optional[str] = None
    ward: Optional[str] = None
    code_type: str
    severity: str
    amount: float = 0.0
    status: str = "Activated"
    ts: Optional[datetime] = None

    @field_validator("patient_id", "department_id")
    @classmethod
    def non_empty(cls, v):
        if not v.strip():
            raise ValueError("cannot be empty")
        return v.strip()

    @field_validator("code_type")
    @classmethod
    def valid_code_type(cls, v):
        if v not in VALID_ICU_TYPES:
            raise ValueError(f"must be one of {sorted(VALID_ICU_TYPES)}")
        return v

    @field_validator("severity")
    @classmethod
    def valid_severity(cls, v):
        if v not in VALID_ICU_SEVERITY:
            raise ValueError(f"must be one of {sorted(VALID_ICU_SEVERITY)}")
        return v

    @field_validator("amount")
    @classmethod
    def valid_amount(cls, v):
        if v < 0:
            raise ValueError("must be non-negative")
        return round(v, 2)

    @field_validator("status")
    @classmethod
    def valid_status(cls, v):
        if v not in VALID_ICU_STATUSES:
            raise ValueError(f"must be one of {sorted(VALID_ICU_STATUSES)}")
        return v


@router.post("/icu-code")
def record_icu_code(body: IcuCodeIn):
    code_id = str(uuid.uuid4())
    ts = body.ts or datetime.utcnow()
    data = body.model_dump()
    data["code_id"] = code_id
    data["ts"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
    return _send("icu_codes", code_id, data)
