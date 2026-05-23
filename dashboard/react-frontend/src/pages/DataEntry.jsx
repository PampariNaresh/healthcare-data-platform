import { useState } from 'react'
import { dataEntry } from '../api/client'
import toast from 'react-hot-toast'

// ── Tab definitions (workflow order) ─────────────────────────────────────────
const TABS = [
  { id: 'patient',       group: 'Clinical',    label: 'Register Patient'     },
  { id: 'doctor',        group: 'Clinical',    label: 'Add Doctor'           },
  { id: 'appointment',   group: 'Clinical',    label: 'Schedule Appointment' },
  { id: 'treatment',     group: 'Clinical',    label: 'Record Treatment'     },
  { id: 'billing',       group: 'Clinical',    label: 'Generate Bill'        },
  { id: 'department',    group: 'Monitoring',  label: 'Add Department'       },
  { id: 'vitals',        group: 'Monitoring',  label: 'Patient Vitals'       },
  { id: 'lab',           group: 'Monitoring',  label: 'Lab Report'           },
  { id: 'hospevent',     group: 'Monitoring',  label: 'Hospital Event'       },
  { id: 'icu',           group: 'Monitoring',  label: 'ICU Code'             },
]

// ── Domain constants (synced with backend) ────────────────────────────────────
const SPECIALIZATIONS = [
  'Cardiology','Dermatology','Gastroenterology','General Medicine',
  'Gynecology','Neurology','Oncology','Orthopedics',
  'Pediatrics','Psychiatry','Radiology','Urology',
]
const BRANCHES = ['Central Hospital','Eastside Clinic','North Wing','South Wing','Westside Clinic']
const TREATMENT_TYPES = [
  'Blood Test','Chemotherapy','Consultation','CT Scan',
  'Dental','ECG','Emergency','MRI','Physiotherapy','Surgery','Vaccination','X-Ray',
]
const PAYMENT_METHODS  = ['Card','Cash','Credit Card','Debit Card','Insurance','Net Banking','UPI']
const PAYMENT_STATUSES = ['Failed','Paid','Pending','Refunded']
const APPT_STATUSES    = ['Cancelled','Completed','No-show','Scheduled']
const WARDS            = ['ER-1','ICU-1','ICU-2','Ward-A','Ward-B']
const LAB_TESTS        = ['Creatinine','Glucose','Hemoglobin','Sodium','Troponin','WBC']
const LAB_UNITS        = { Glucose:'mg/dL', Hemoglobin:'g/dL', WBC:'K/uL', Creatinine:'mg/dL', Troponin:'ng/mL', Sodium:'mEq/L' }
const LAB_RANGES       = { Glucose:'70-100', Hemoglobin:'12-17.5', WBC:'4.5-11.0', Creatinine:'0.6-1.2', Troponin:'0.0-0.04', Sodium:'136-145' }
const LAB_FLAGS        = ['critical','high','low','normal']
const EVENT_TYPES      = ['Admission','Discharge','Emergency_Arrival','ICU_Transfer','Surgery_End','Surgery_Start','Transfer']
const ICU_TYPES        = ['Code_Blue','Rapid_Response','STEMI_Alert','Stroke_Alert','Trauma_Activation']
const ICU_SEVERITY_MAP = { Code_Blue:'CRITICAL', STEMI_Alert:'CRITICAL', Stroke_Alert:'CRITICAL', Rapid_Response:'HIGH', Trauma_Activation:'CRITICAL' }
const ICU_STATUSES     = ['Activated','False_Alarm','Resolved']

// ── Helpers ───────────────────────────────────────────────────────────────────
function parseApiError(err) {
  const detail = err.response?.data?.detail
  if (!detail) return err.message || 'An error occurred'
  if (typeof detail === 'string') return detail
  if (Array.isArray(detail)) {
    return detail.map(d => {
      const field = d.loc?.slice(-1)[0] ?? 'field'
      return `${field}: ${d.msg}`
    }).join(' · ')
  }
  return String(detail)
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
const PHONE_RE = /^\d{10}$/
function digits(v) { return v.replace(/\D/g, '') }

// ── Shared UI ─────────────────────────────────────────────────────────────────
function Field({ label, required, error, children }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-400 mb-1">
        {label}{required && <span className="text-red-400 ml-0.5">*</span>}
      </label>
      {children}
      {error && <p className="mt-1 text-xs text-red-400">{error}</p>}
    </div>
  )
}

const base = 'w-full bg-navy-900 border rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:ring-1 transition'
const inp  = (e) => `${base} ${e ? 'border-red-500 focus:border-red-500 focus:ring-red-500' : 'border-navy-700 focus:border-brand-500 focus:ring-brand-500'}`
const sel  = (e) => `${inp(e)} cursor-pointer`

function Btn({ loading, label }) {
  return (
    <button type="submit" disabled={loading}
      className="bg-brand-600 hover:bg-brand-500 disabled:opacity-60 disabled:cursor-not-allowed text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors min-w-[160px]">
      {loading ? 'Submitting…' : label}
    </button>
  )
}

// ── 1. Patient ────────────────────────────────────────────────────────────────
const INIT_PAT = { first_name:'', last_name:'', gender:'', date_of_birth:'', contact_number:'', address:'', insurance_provider:'', insurance_number:'', email:'' }

function validatePatient(f) {
  const e = {}
  if (f.first_name.trim().length < 2)  e.first_name    = 'At least 2 characters'
  if (f.last_name.trim().length < 2)   e.last_name     = 'At least 2 characters'
  if (!f.gender)                        e.gender        = 'Select a gender'
  if (!f.date_of_birth) {
    e.date_of_birth = 'Required'
  } else {
    const dob = new Date(f.date_of_birth), today = new Date()
    if (dob >= today)                              e.date_of_birth = 'Must be in the past'
    else if ((today - dob) / 31557600000 > 120)   e.date_of_birth = 'Invalid date of birth'
  }
  if (!PHONE_RE.test(digits(f.contact_number))) e.contact_number = 'Exactly 10 digits'
  if (f.email && !EMAIL_RE.test(f.email))       e.email          = 'Invalid email'
  return e
}

function PatientForm() {
  const [f, setF] = useState(INIT_PAT)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validatePatient(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      const res = await dataEntry.patient({ ...f, contact_number: digits(f.contact_number) })
      toast.success(`Patient registered — ID: ${res.data?.topic ? '' : ''}${f.first_name} ${f.last_name}`)
      setF(INIT_PAT); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required error={err.first_name}>
          <input className={inp(err.first_name)} value={f.first_name} onChange={set('first_name')} placeholder="First name" />
        </Field>
        <Field label="Last Name" required error={err.last_name}>
          <input className={inp(err.last_name)} value={f.last_name} onChange={set('last_name')} placeholder="Last name" />
        </Field>
        <Field label="Date of Birth" required error={err.date_of_birth}>
          <input type="date" className={inp(err.date_of_birth)} value={f.date_of_birth} onChange={set('date_of_birth')} />
        </Field>
        <Field label="Gender" required error={err.gender}>
          <select className={sel(err.gender)} value={f.gender} onChange={set('gender')}>
            <option value="">Select gender</option>
            <option value="M">Male</option>
            <option value="F">Female</option>
          </select>
        </Field>
        <Field label="Contact Number" required error={err.contact_number}>
          <input className={inp(err.contact_number)} value={f.contact_number} onChange={set('contact_number')} placeholder="10-digit mobile" />
        </Field>
        <Field label="Email" error={err.email}>
          <input type="email" className={inp(err.email)} value={f.email} onChange={set('email')} placeholder="patient@email.com" />
        </Field>
        <Field label="Address">
          <input className={inp()} value={f.address} onChange={set('address')} placeholder="Street address" />
        </Field>
        <Field label="Insurance Provider">
          <input className={inp()} value={f.insurance_provider} onChange={set('insurance_provider')} placeholder="e.g. Star Health" />
        </Field>
        <Field label="Insurance Number">
          <input className={inp()} value={f.insurance_number} onChange={set('insurance_number')} placeholder="Policy number" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Register Patient" /></div>
    </form>
  )
}

// ── 2. Doctor ─────────────────────────────────────────────────────────────────
const INIT_DOC = { first_name:'', last_name:'', specialization:'', phone_number:'', years_experience:'', hospital_branch:'', email:'' }

function validateDoctor(f) {
  const e = {}
  if (f.first_name.trim().length < 2)   e.first_name     = 'At least 2 characters'
  if (f.last_name.trim().length < 2)    e.last_name      = 'At least 2 characters'
  if (!f.specialization)                e.specialization = 'Select a specialization'
  if (!PHONE_RE.test(digits(f.phone_number))) e.phone_number = 'Exactly 10 digits'
  const yoe = Number(f.years_experience)
  if (f.years_experience === '' || isNaN(yoe) || yoe < 0 || yoe > 60) e.years_experience = '0 – 60'
  if (f.email && !EMAIL_RE.test(f.email)) e.email = 'Invalid email'
  return e
}

function DoctorForm() {
  const [f, setF] = useState(INIT_DOC)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateDoctor(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.doctor({ ...f, phone_number: digits(f.phone_number), years_experience: Number(f.years_experience) })
      toast.success('Doctor added successfully')
      setF(INIT_DOC); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required error={err.first_name}>
          <input className={inp(err.first_name)} value={f.first_name} onChange={set('first_name')} placeholder="First name" />
        </Field>
        <Field label="Last Name" required error={err.last_name}>
          <input className={inp(err.last_name)} value={f.last_name} onChange={set('last_name')} placeholder="Last name" />
        </Field>
        <Field label="Specialization" required error={err.specialization}>
          <select className={sel(err.specialization)} value={f.specialization} onChange={set('specialization')}>
            <option value="">Select specialization</option>
            {SPECIALIZATIONS.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Hospital Branch">
          <select className={sel()} value={f.hospital_branch} onChange={set('hospital_branch')}>
            <option value="">Select branch</option>
            {BRANCHES.map(b => <option key={b}>{b}</option>)}
          </select>
        </Field>
        <Field label="Phone Number" required error={err.phone_number}>
          <input className={inp(err.phone_number)} value={f.phone_number} onChange={set('phone_number')} placeholder="10-digit mobile" />
        </Field>
        <Field label="Years of Experience" required error={err.years_experience}>
          <input type="number" min={0} max={60} className={inp(err.years_experience)} value={f.years_experience} onChange={set('years_experience')} placeholder="0 – 60" />
        </Field>
        <Field label="Email" error={err.email}>
          <input type="email" className={inp(err.email)} value={f.email} onChange={set('email')} placeholder="doctor@hospital.com" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Add Doctor" /></div>
    </form>
  )
}

// ── 3. Appointment ────────────────────────────────────────────────────────────
const INIT_APPT = { patient_id:'', doctor_id:'', appointment_date:'', appointment_time:'', reason_for_visit:'', status:'Scheduled' }

function validateAppt(f) {
  const e = {}
  if (!f.patient_id.trim())    e.patient_id       = 'Required (e.g. P-3A1B2C)'
  if (!f.doctor_id.trim())     e.doctor_id        = 'Required (e.g. D-4F5E6D)'
  if (!f.appointment_date)     e.appointment_date = 'Required'
  if (!f.appointment_time)     e.appointment_time = 'Required'
  return e
}

function AppointmentForm() {
  const [f, setF] = useState(INIT_APPT)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateAppt(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.appointment(f)
      toast.success('Appointment scheduled')
      setF(INIT_APPT); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" />
        </Field>
        <Field label="Doctor ID" required error={err.doctor_id}>
          <input className={inp(err.doctor_id)} value={f.doctor_id} onChange={set('doctor_id')} placeholder="e.g. D-4F5E6D" />
        </Field>
        <Field label="Appointment Date" required error={err.appointment_date}>
          <input type="date" className={inp(err.appointment_date)} value={f.appointment_date} onChange={set('appointment_date')} />
        </Field>
        <Field label="Appointment Time" required error={err.appointment_time}>
          <input type="time" className={inp(err.appointment_time)} value={f.appointment_time} onChange={set('appointment_time')} />
        </Field>
        <Field label="Status">
          <select className={sel()} value={f.status} onChange={set('status')}>
            {APPT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Reason for Visit">
          <input className={inp()} value={f.reason_for_visit} onChange={set('reason_for_visit')} placeholder="Chief complaint" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Schedule Appointment" /></div>
    </form>
  )
}

// ── 4. Treatment ──────────────────────────────────────────────────────────────
const INIT_TREAT = { appointment_id:'', treatment_type:'', description:'', cost:'', treatment_date:'' }

function validateTreat(f) {
  const e = {}
  if (!f.appointment_id.trim()) e.appointment_id = 'Required (e.g. A-7G8H9I)'
  if (!f.treatment_type)        e.treatment_type = 'Select a type'
  if (!f.treatment_date)        e.treatment_date = 'Required'
  const cost = Number(f.cost)
  if (f.cost === '' || isNaN(cost) || cost <= 0) e.cost = 'Must be > 0'
  return e
}

function TreatmentForm() {
  const [f, setF] = useState(INIT_TREAT)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateTreat(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.treatment({ ...f, cost: Number(f.cost) })
      toast.success('Treatment recorded')
      setF(INIT_TREAT); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Appointment ID" required error={err.appointment_id}>
          <input className={inp(err.appointment_id)} value={f.appointment_id} onChange={set('appointment_id')} placeholder="e.g. A-7G8H9I" />
        </Field>
        <Field label="Treatment Type" required error={err.treatment_type}>
          <select className={sel(err.treatment_type)} value={f.treatment_type} onChange={set('treatment_type')}>
            <option value="">Select type</option>
            {TREATMENT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Treatment Date" required error={err.treatment_date}>
          <input type="date" className={inp(err.treatment_date)} value={f.treatment_date} onChange={set('treatment_date')} />
        </Field>
        <Field label="Cost (₹)" required error={err.cost}>
          <input type="number" min={0.01} step="0.01" className={inp(err.cost)} value={f.cost} onChange={set('cost')} placeholder="0.00" />
        </Field>
        <Field label="Description">
          <input className={inp()} value={f.description} onChange={set('description')} placeholder="Notes" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Record Treatment" /></div>
    </form>
  )
}

// ── 5. Billing ────────────────────────────────────────────────────────────────
const INIT_BILL = { patient_id:'', treatment_id:'', bill_date:'', amount:'', payment_method:'', payment_status:'Pending' }

function validateBill(f) {
  const e = {}
  if (!f.patient_id.trim())   e.patient_id   = 'Required'
  if (!f.treatment_id.trim()) e.treatment_id = 'Required'
  if (!f.bill_date)           e.bill_date    = 'Required'
  const amt = Number(f.amount)
  if (f.amount === '' || isNaN(amt) || amt <= 0) e.amount = 'Must be > 0'
  return e
}

function BillingForm() {
  const [f, setF] = useState(INIT_BILL)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateBill(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.billing({ ...f, amount: Number(f.amount) })
      toast.success('Bill generated successfully')
      setF(INIT_BILL); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" />
        </Field>
        <Field label="Treatment ID" required error={err.treatment_id}>
          <input className={inp(err.treatment_id)} value={f.treatment_id} onChange={set('treatment_id')} placeholder="e.g. T-1J2K3L" />
        </Field>
        <Field label="Bill Date" required error={err.bill_date}>
          <input type="date" className={inp(err.bill_date)} value={f.bill_date} onChange={set('bill_date')} />
        </Field>
        <Field label="Amount (₹)" required error={err.amount}>
          <input type="number" min={0.01} step="0.01" className={inp(err.amount)} value={f.amount} onChange={set('amount')} placeholder="0.00" />
        </Field>
        <Field label="Payment Method">
          <select className={sel()} value={f.payment_method} onChange={set('payment_method')}>
            <option value="">Select method</option>
            {PAYMENT_METHODS.map(m => <option key={m}>{m}</option>)}
          </select>
        </Field>
        <Field label="Payment Status" error={err.payment_status}>
          <select className={sel(err.payment_status)} value={f.payment_status} onChange={set('payment_status')}>
            <option value="">Select status</option>
            {PAYMENT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Generate Bill" /></div>
    </form>
  )
}

// ── 6. Department ─────────────────────────────────────────────────────────────
const INIT_DEPT = { department_id:'', department_name:'', hospital_branch:'' }

function validateDept(f) {
  const e = {}
  if (f.department_id.trim().length < 2)   e.department_id   = 'At least 2 characters (e.g. DEPT06)'
  if (f.department_name.trim().length < 2)  e.department_name = 'At least 2 characters'
  return e
}

function DepartmentForm() {
  const [f, setF] = useState(INIT_DEPT)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateDept(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.department(f)
      toast.success(`Department ${f.department_id} added`)
      setF(INIT_DEPT); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <p className="text-xs text-slate-400">
        Pre-seeded IDs: DEPT01 (ICU), DEPT02 (Cardiology), DEPT03 (Emergency), DEPT04 (Nephrology), DEPT05 (Neurology).
        Use a new ID to add additional departments.
      </p>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Department ID" required error={err.department_id}>
          <input className={inp(err.department_id)} value={f.department_id} onChange={set('department_id')} placeholder="e.g. DEPT06" />
        </Field>
        <Field label="Department Name" required error={err.department_name}>
          <input className={inp(err.department_name)} value={f.department_name} onChange={set('department_name')} placeholder="e.g. Oncology" />
        </Field>
        <Field label="Hospital Branch">
          <select className={sel()} value={f.hospital_branch} onChange={set('hospital_branch')}>
            <option value="">Select branch</option>
            {BRANCHES.map(b => <option key={b}>{b}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Add Department" /></div>
    </form>
  )
}

// ── 7. Patient Vitals ─────────────────────────────────────────────────────────
const INIT_VITALS = { patient_id:'', hospital:'City General Hospital', ward:'', heart_rate:'', spo2:'', systolic:'', diastolic:'', temperature_celsius:'', respiratory_rate:'', is_anomaly:false }

function validateVitals(f) {
  const e = {}
  if (!f.patient_id.trim())   e.patient_id = 'Required'
  const hr = Number(f.heart_rate)
  if (f.heart_rate === '' || isNaN(hr) || hr < 30 || hr > 250)   e.heart_rate = '30 – 250'
  const sp = Number(f.spo2)
  if (f.spo2 === '' || isNaN(sp) || sp < 50 || sp > 100)         e.spo2       = '50.0 – 100.0'
  const sy = Number(f.systolic)
  if (f.systolic === '' || isNaN(sy) || sy < 50 || sy > 250)     e.systolic   = '50 – 250'
  const di = Number(f.diastolic)
  if (f.diastolic === '' || isNaN(di) || di < 30 || di > 150)    e.diastolic  = '30 – 150'
  const tc = Number(f.temperature_celsius)
  if (f.temperature_celsius === '' || isNaN(tc) || tc < 33 || tc > 45) e.temperature_celsius = '33.0 – 45.0 °C'
  const rr = Number(f.respiratory_rate)
  if (f.respiratory_rate === '' || isNaN(rr) || rr < 5 || rr > 60)    e.respiratory_rate    = '5 – 60'
  return e
}

function VitalsForm() {
  const [f, setF] = useState(INIT_VITALS)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))
  const setCheck = k => ev => setF(p => ({ ...p, [k]: ev.target.checked }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateVitals(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.patientVitals({
        ...f,
        heart_rate: Number(f.heart_rate), spo2: Number(f.spo2),
        systolic: Number(f.systolic), diastolic: Number(f.diastolic),
        temperature_celsius: Number(f.temperature_celsius),
        respiratory_rate: Number(f.respiratory_rate),
      })
      toast.success('Vitals recorded')
      setF(INIT_VITALS); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C or P001" />
        </Field>
        <Field label="Hospital">
          <input className={inp()} value={f.hospital} onChange={set('hospital')} placeholder="Hospital name" />
        </Field>
        <Field label="Ward">
          <select className={sel()} value={f.ward} onChange={set('ward')}>
            <option value="">Select ward</option>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Heart Rate (bpm)" required error={err.heart_rate}>
          <input type="number" className={inp(err.heart_rate)} value={f.heart_rate} onChange={set('heart_rate')} placeholder="30 – 250" />
        </Field>
        <Field label="SpO2 (%)" required error={err.spo2}>
          <input type="number" step="0.1" className={inp(err.spo2)} value={f.spo2} onChange={set('spo2')} placeholder="50.0 – 100.0" />
        </Field>
        <Field label="Systolic BP (mmHg)" required error={err.systolic}>
          <input type="number" className={inp(err.systolic)} value={f.systolic} onChange={set('systolic')} placeholder="50 – 250" />
        </Field>
        <Field label="Diastolic BP (mmHg)" required error={err.diastolic}>
          <input type="number" className={inp(err.diastolic)} value={f.diastolic} onChange={set('diastolic')} placeholder="30 – 150" />
        </Field>
        <Field label="Temperature (°C)" required error={err.temperature_celsius}>
          <input type="number" step="0.1" className={inp(err.temperature_celsius)} value={f.temperature_celsius} onChange={set('temperature_celsius')} placeholder="33.0 – 45.0" />
        </Field>
        <Field label="Respiratory Rate (breaths/min)" required error={err.respiratory_rate}>
          <input type="number" className={inp(err.respiratory_rate)} value={f.respiratory_rate} onChange={set('respiratory_rate')} placeholder="5 – 60" />
        </Field>
        <Field label="Anomaly Flag">
          <label className="flex items-center gap-2 cursor-pointer mt-2">
            <input type="checkbox" checked={f.is_anomaly} onChange={setCheck('is_anomaly')} className="w-4 h-4 accent-red-500" />
            <span className="text-sm text-slate-300">Mark as anomaly</span>
          </label>
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Record Vitals" /></div>
    </form>
  )
}

// ── 8. Lab Report ─────────────────────────────────────────────────────────────
const INIT_LAB = { patient_id:'', doctor_id:'', hospital:'City General Hospital', test_name:'', value:'', unit:'', normal_range:'', flag:'', amount:'' }

function validateLab(f) {
  const e = {}
  if (!f.patient_id.trim()) e.patient_id = 'Required'
  if (!f.doctor_id.trim())  e.doctor_id  = 'Required'
  if (!f.test_name)         e.test_name  = 'Select a test'
  const val = Number(f.value)
  if (f.value === '' || isNaN(val) || val < 0) e.value = 'Must be ≥ 0'
  if (!f.flag)              e.flag   = 'Select a flag'
  const amt = Number(f.amount)
  if (f.amount === '' || isNaN(amt) || amt < 0) e.amount = 'Must be ≥ 0'
  return e
}

function LabReportForm() {
  const [f, setF] = useState(INIT_LAB)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => {
    const val = ev.target.value
    setF(p => {
      const next = { ...p, [k]: val }
      if (k === 'test_name' && val) {
        next.unit         = LAB_UNITS[val]  || ''
        next.normal_range = LAB_RANGES[val] || ''
      }
      return next
    })
  }

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateLab(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.labReport({ ...f, value: Number(f.value), amount: Number(f.amount) })
      toast.success('Lab report recorded')
      setF(INIT_LAB); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C or P001" />
        </Field>
        <Field label="Doctor ID" required error={err.doctor_id}>
          <input className={inp(err.doctor_id)} value={f.doctor_id} onChange={set('doctor_id')} placeholder="e.g. D-4F5E6D or D001" />
        </Field>
        <Field label="Hospital">
          <input className={inp()} value={f.hospital} onChange={set('hospital')} placeholder="Hospital name" />
        </Field>
        <Field label="Test Name" required error={err.test_name}>
          <select className={sel(err.test_name)} value={f.test_name} onChange={set('test_name')}>
            <option value="">Select test</option>
            {LAB_TESTS.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Result Value" required error={err.value}>
          <input type="number" step="0.001" className={inp(err.value)} value={f.value} onChange={set('value')} placeholder="Numeric result" />
        </Field>
        <Field label="Unit">
          <input className={inp()} value={f.unit} onChange={set('unit')} placeholder="e.g. mg/dL" />
        </Field>
        <Field label="Normal Range">
          <input className={inp()} value={f.normal_range} onChange={set('normal_range')} placeholder="e.g. 70-100" />
        </Field>
        <Field label="Flag" required error={err.flag}>
          <select className={sel(err.flag)} value={f.flag} onChange={set('flag')}>
            <option value="">Select flag</option>
            {LAB_FLAGS.map(fl => <option key={fl}>{fl}</option>)}
          </select>
        </Field>
        <Field label="Amount (₹)" required error={err.amount}>
          <input type="number" min={0} step="0.01" className={inp(err.amount)} value={f.amount} onChange={set('amount')} placeholder="0.00" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Record Lab Report" /></div>
    </form>
  )
}

// ── 9. Hospital Event ─────────────────────────────────────────────────────────
const INIT_HEVT = { patient_id:'', department_id:'', hospital:'City General Hospital', ward:'', event_type:'', amount:'' }

function validateHEvent(f) {
  const e = {}
  if (!f.patient_id.trim())    e.patient_id    = 'Required'
  if (!f.department_id.trim()) e.department_id = 'Required (e.g. DEPT01)'
  if (!f.event_type)           e.event_type    = 'Select an event type'
  const amt = Number(f.amount)
  if (f.amount === '' || isNaN(amt) || amt < 0) e.amount = 'Must be ≥ 0'
  return e
}

function HospitalEventForm() {
  const [f, setF] = useState(INIT_HEVT)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => setF(p => ({ ...p, [k]: ev.target.value }))

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateHEvent(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.hospitalEvent({ ...f, amount: Number(f.amount) })
      toast.success('Hospital event recorded')
      setF(INIT_HEVT); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P001" />
        </Field>
        <Field label="Department ID" required error={err.department_id}>
          <input className={inp(err.department_id)} value={f.department_id} onChange={set('department_id')} placeholder="e.g. DEPT01" />
        </Field>
        <Field label="Hospital">
          <input className={inp()} value={f.hospital} onChange={set('hospital')} placeholder="Hospital name" />
        </Field>
        <Field label="Ward">
          <select className={sel()} value={f.ward} onChange={set('ward')}>
            <option value="">Select ward</option>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Event Type" required error={err.event_type}>
          <select className={sel(err.event_type)} value={f.event_type} onChange={set('event_type')}>
            <option value="">Select event type</option>
            {EVENT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Amount (₹)" required error={err.amount}>
          <input type="number" min={0} step="0.01" className={inp(err.amount)} value={f.amount} onChange={set('amount')} placeholder="0.00" />
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Record Event" /></div>
    </form>
  )
}

// ── 10. ICU Code ──────────────────────────────────────────────────────────────
const INIT_ICU = { patient_id:'', department_id:'', hospital:'City General Hospital', ward:'', code_type:'', severity:'', amount:'', status:'Activated' }

function validateIcu(f) {
  const e = {}
  if (!f.patient_id.trim())    e.patient_id    = 'Required'
  if (!f.department_id.trim()) e.department_id = 'Required (e.g. DEPT01)'
  if (!f.code_type)            e.code_type     = 'Select a code type'
  if (!f.severity)             e.severity      = 'Select severity'
  const amt = Number(f.amount)
  if (f.amount === '' || isNaN(amt) || amt < 0) e.amount = 'Must be ≥ 0'
  return e
}

function IcuCodeForm() {
  const [f, setF] = useState(INIT_ICU)
  const [err, setErr] = useState({})
  const [loading, setLoading] = useState(false)
  const set = k => ev => {
    const val = ev.target.value
    setF(p => {
      const next = { ...p, [k]: val }
      if (k === 'code_type' && val) {
        next.severity = ICU_SEVERITY_MAP[val] || ''
      }
      return next
    })
  }

  const submit = async (ev) => {
    ev.preventDefault()
    const e = validateIcu(f); if (Object.keys(e).length) { setErr(e); return }
    setLoading(true)
    try {
      await dataEntry.icuCode({ ...f, amount: Number(f.amount) })
      toast.success('ICU code recorded')
      setF(INIT_ICU); setErr({})
    } catch (ex) { toast.error(parseApiError(ex)) }
    finally { setLoading(false) }
  }

  return (
    <form onSubmit={submit} noValidate className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required error={err.patient_id}>
          <input className={inp(err.patient_id)} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P001" />
        </Field>
        <Field label="Department ID" required error={err.department_id}>
          <input className={inp(err.department_id)} value={f.department_id} onChange={set('department_id')} placeholder="e.g. DEPT01" />
        </Field>
        <Field label="Hospital">
          <input className={inp()} value={f.hospital} onChange={set('hospital')} placeholder="Hospital name" />
        </Field>
        <Field label="Ward">
          <select className={sel()} value={f.ward} onChange={set('ward')}>
            <option value="">Select ward</option>
            {WARDS.map(w => <option key={w}>{w}</option>)}
          </select>
        </Field>
        <Field label="Code Type" required error={err.code_type}>
          <select className={sel(err.code_type)} value={f.code_type} onChange={set('code_type')}>
            <option value="">Select code type</option>
            {ICU_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Severity" required error={err.severity}>
          <select className={sel(err.severity)} value={f.severity} onChange={set('severity')}>
            <option value="">Select severity</option>
            <option value="CRITICAL">CRITICAL</option>
            <option value="HIGH">HIGH</option>
          </select>
        </Field>
        <Field label="Amount (₹)" required error={err.amount}>
          <input type="number" min={0} step="0.01" className={inp(err.amount)} value={f.amount} onChange={set('amount')} placeholder="0.00" />
        </Field>
        <Field label="Status">
          <select className={sel()} value={f.status} onChange={set('status')}>
            {ICU_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2"><Btn loading={loading} label="Record ICU Code" /></div>
    </form>
  )
}

// ── Page ──────────────────────────────────────────────────────────────────────
const FORMS = {
  patient: PatientForm, doctor: DoctorForm, appointment: AppointmentForm,
  treatment: TreatmentForm, billing: BillingForm,
  department: DepartmentForm, vitals: VitalsForm, lab: LabReportForm,
  hospevent: HospitalEventForm, icu: IcuCodeForm,
}

const GROUPS = ['Clinical', 'Monitoring']

export default function DataEntry() {
  const [active, setActive] = useState('patient')
  const ActiveForm = FORMS[active]
  const activeLabel = TABS.find(t => t.id === active)?.label

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Data Entry</h1>

      {GROUPS.map(group => (
        <div key={group}>
          <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2 px-1">{group}</p>
          <div className="flex gap-1 bg-navy-800 p-1 rounded-xl flex-wrap">
            {TABS.filter(t => t.group === group).map(({ id, label }) => (
              <button
                key={id}
                onClick={() => setActive(id)}
                className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors ${
                  active === id ? 'bg-brand-600 text-white' : 'text-slate-400 hover:text-white hover:bg-navy-700'
                }`}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
      ))}

      <div className="bg-navy-800 rounded-xl border border-navy-700 p-6">
        <h2 className="text-base font-semibold text-white mb-5">{activeLabel}</h2>
        <ActiveForm />
      </div>
    </div>
  )
}
