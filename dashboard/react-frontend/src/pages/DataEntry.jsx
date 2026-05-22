import { useState } from 'react'
import { dataEntry } from '../api/client'
import toast from 'react-hot-toast'

const TABS = [
  { id: 'patient',     label: 'Register Patient'    },
  { id: 'doctor',      label: 'Add Doctor'          },
  { id: 'appointment', label: 'Schedule Appointment'},
  { id: 'treatment',   label: 'Record Treatment'    },
  { id: 'billing',     label: 'Generate Bill'       },
]

const SPECIALIZATIONS = ['Cardiology','Orthopedics','Neurology','Dermatology','Pediatrics','Oncology','General Medicine','Gynecology']
const BRANCHES         = ['Main Hospital','North Branch','South Branch','East Branch','West Branch']
const TREATMENT_TYPES  = ['Consultation','Surgery','Physiotherapy','Laboratory Test','Radiology','Vaccination','Dental','Emergency']
const PAYMENT_METHODS  = ['Cash','Credit Card','Debit Card','Insurance','UPI','Net Banking']
const PAYMENT_STATUSES = ['Paid','Pending','Failed','Refunded']
const APPT_STATUSES    = ['Scheduled','Completed','Cancelled','No-show']

function Field({ label, required, children }) {
  return (
    <div>
      <label className="block text-xs font-medium text-slate-400 mb-1">
        {label}{required && <span className="text-red-400 ml-0.5">*</span>}
      </label>
      {children}
    </div>
  )
}

const inp = 'w-full bg-navy-900 border border-navy-700 rounded-lg px-3 py-2 text-sm text-white placeholder-slate-600 focus:outline-none focus:border-brand-500 focus:ring-1 focus:ring-brand-500 transition'
const sel = `${inp} cursor-pointer`

function PatientForm() {
  const [f, setF] = useState({ first_name:'', last_name:'', gender:'', date_of_birth:'', contact_number:'', address:'', insurance_provider:'', insurance_number:'', email:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.patient(f)
      toast.success('Patient registered successfully')
      setF({ first_name:'', last_name:'', gender:'', date_of_birth:'', contact_number:'', address:'', insurance_provider:'', insurance_number:'', email:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Registration failed')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required>
          <input className={inp} value={f.first_name} onChange={set('first_name')} placeholder="Enter first name" required />
        </Field>
        <Field label="Last Name" required>
          <input className={inp} value={f.last_name} onChange={set('last_name')} placeholder="Enter last name" required />
        </Field>
        <Field label="Date of Birth" required>
          <input type="date" className={inp} value={f.date_of_birth} onChange={set('date_of_birth')} required />
        </Field>
        <Field label="Gender" required>
          <select className={sel} value={f.gender} onChange={set('gender')} required>
            <option value="">Select gender</option>
            <option value="M">Male</option>
            <option value="F">Female</option>
            <option value="O">Other</option>
          </select>
        </Field>
        <Field label="Contact Number" required>
          <input className={inp} value={f.contact_number} onChange={set('contact_number')} placeholder="+91 XXXXX XXXXX" required />
        </Field>
        <Field label="Email">
          <input type="email" className={inp} value={f.email} onChange={set('email')} placeholder="patient@email.com" />
        </Field>
        <Field label="Address">
          <input className={inp} value={f.address} onChange={set('address')} placeholder="Street address" />
        </Field>
        <Field label="Insurance Provider">
          <input className={inp} value={f.insurance_provider} onChange={set('insurance_provider')} placeholder="e.g. Star Health" />
        </Field>
        <Field label="Insurance Number">
          <input className={inp} value={f.insurance_number} onChange={set('insurance_number')} placeholder="Policy number" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Register Patient
        </button>
      </div>
    </form>
  )
}

function DoctorForm() {
  const [f, setF] = useState({ first_name:'', last_name:'', specialization:'', phone_number:'', years_experience:0, hospital_branch:'', email:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.doctor({ ...f, years_experience: Number(f.years_experience) })
      toast.success('Doctor added successfully')
      setF({ first_name:'', last_name:'', specialization:'', phone_number:'', years_experience:0, hospital_branch:'', email:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to add doctor')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="First Name" required>
          <input className={inp} value={f.first_name} onChange={set('first_name')} placeholder="Dr. first name" required />
        </Field>
        <Field label="Last Name" required>
          <input className={inp} value={f.last_name} onChange={set('last_name')} placeholder="Dr. last name" required />
        </Field>
        <Field label="Specialization" required>
          <select className={sel} value={f.specialization} onChange={set('specialization')} required>
            <option value="">Select specialization</option>
            {SPECIALIZATIONS.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Hospital Branch">
          <select className={sel} value={f.hospital_branch} onChange={set('hospital_branch')}>
            <option value="">Select branch</option>
            {BRANCHES.map(b => <option key={b}>{b}</option>)}
          </select>
        </Field>
        <Field label="Phone Number" required>
          <input className={inp} value={f.phone_number} onChange={set('phone_number')} placeholder="+91 XXXXX XXXXX" required />
        </Field>
        <Field label="Years of Experience" required>
          <input type="number" min={0} max={60} className={inp} value={f.years_experience} onChange={set('years_experience')} required />
        </Field>
        <Field label="Email">
          <input type="email" className={inp} value={f.email} onChange={set('email')} placeholder="doctor@hospital.com" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Add Doctor
        </button>
      </div>
    </form>
  )
}

function AppointmentForm() {
  const [f, setF] = useState({ patient_id:'', doctor_id:'', appointment_date:'', appointment_time:'', reason_for_visit:'', status:'Scheduled' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.appointment(f)
      toast.success('Appointment scheduled')
      setF({ patient_id:'', doctor_id:'', appointment_date:'', appointment_time:'', reason_for_visit:'', status:'Scheduled' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to schedule')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" required />
        </Field>
        <Field label="Doctor ID" required>
          <input className={inp} value={f.doctor_id} onChange={set('doctor_id')} placeholder="e.g. D-4F5E6D" required />
        </Field>
        <Field label="Appointment Date" required>
          <input type="date" className={inp} value={f.appointment_date} onChange={set('appointment_date')} required />
        </Field>
        <Field label="Appointment Time" required>
          <input type="time" className={inp} value={f.appointment_time} onChange={set('appointment_time')} required />
        </Field>
        <Field label="Status">
          <select className={sel} value={f.status} onChange={set('status')}>
            {APPT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
        <Field label="Reason for Visit">
          <input className={inp} value={f.reason_for_visit} onChange={set('reason_for_visit')} placeholder="Chief complaint" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Schedule Appointment
        </button>
      </div>
    </form>
  )
}

function TreatmentForm() {
  const [f, setF] = useState({ appointment_id:'', treatment_type:'', description:'', cost:'', treatment_date:'' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.treatment({ ...f, cost: Number(f.cost) })
      toast.success('Treatment recorded')
      setF({ appointment_id:'', treatment_type:'', description:'', cost:'', treatment_date:'' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to record treatment')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Appointment ID" required>
          <input className={inp} value={f.appointment_id} onChange={set('appointment_id')} placeholder="e.g. A-7G8H9I" required />
        </Field>
        <Field label="Treatment Type" required>
          <select className={sel} value={f.treatment_type} onChange={set('treatment_type')} required>
            <option value="">Select type</option>
            {TREATMENT_TYPES.map(t => <option key={t}>{t}</option>)}
          </select>
        </Field>
        <Field label="Treatment Date" required>
          <input type="date" className={inp} value={f.treatment_date} onChange={set('treatment_date')} required />
        </Field>
        <Field label="Cost (₹)" required>
          <input type="number" min={0} step="0.01" className={inp} value={f.cost} onChange={set('cost')} placeholder="0.00" required />
        </Field>
        <Field label="Description">
          <input className={inp} value={f.description} onChange={set('description')} placeholder="Treatment notes" />
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Record Treatment
        </button>
      </div>
    </form>
  )
}

function BillingForm() {
  const [f, setF] = useState({ patient_id:'', treatment_id:'', bill_date:'', amount:'', payment_method:'', payment_status:'Pending' })
  const set = k => e => setF(p => ({ ...p, [k]: e.target.value }))

  const submit = async (e) => {
    e.preventDefault()
    try {
      await dataEntry.billing({ ...f, amount: Number(f.amount) })
      toast.success('Bill generated successfully')
      setF({ patient_id:'', treatment_id:'', bill_date:'', amount:'', payment_method:'', payment_status:'Pending' })
    } catch (err) {
      toast.error(err.response?.data?.detail || 'Failed to generate bill')
    }
  }

  return (
    <form onSubmit={submit} className="space-y-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Field label="Patient ID" required>
          <input className={inp} value={f.patient_id} onChange={set('patient_id')} placeholder="e.g. P-3A1B2C" required />
        </Field>
        <Field label="Treatment ID" required>
          <input className={inp} value={f.treatment_id} onChange={set('treatment_id')} placeholder="e.g. T-1J2K3L" required />
        </Field>
        <Field label="Bill Date" required>
          <input type="date" className={inp} value={f.bill_date} onChange={set('bill_date')} required />
        </Field>
        <Field label="Amount (₹)" required>
          <input type="number" min={0} step="0.01" className={inp} value={f.amount} onChange={set('amount')} placeholder="0.00" required />
        </Field>
        <Field label="Payment Method">
          <select className={sel} value={f.payment_method} onChange={set('payment_method')}>
            <option value="">Select method</option>
            {PAYMENT_METHODS.map(m => <option key={m}>{m}</option>)}
          </select>
        </Field>
        <Field label="Payment Status">
          <select className={sel} value={f.payment_status} onChange={set('payment_status')}>
            {PAYMENT_STATUSES.map(s => <option key={s}>{s}</option>)}
          </select>
        </Field>
      </div>
      <div className="flex justify-end pt-2">
        <button type="submit" className="bg-brand-600 hover:bg-brand-500 text-white font-medium px-6 py-2.5 rounded-lg text-sm transition-colors">
          Generate Bill
        </button>
      </div>
    </form>
  )
}

const FORMS = { patient: PatientForm, doctor: DoctorForm, appointment: AppointmentForm, treatment: TreatmentForm, billing: BillingForm }

export default function DataEntry() {
  const [active, setActive] = useState('patient')
  const ActiveForm = FORMS[active]

  return (
    <div className="space-y-6">
      <h1 className="text-xl font-bold text-white">Data Entry</h1>

      {/* Tabs */}
      <div className="flex gap-1 bg-navy-800 p-1 rounded-xl w-fit flex-wrap">
        {TABS.map(({ id, label }) => (
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

      {/* Form card */}
      <div className="bg-navy-800 rounded-xl border border-navy-700 p-6">
        <h2 className="text-base font-semibold text-white mb-5">
          {TABS.find(t => t.id === active)?.label}
        </h2>
        <ActiveForm />
      </div>
    </div>
  )
}
