# Healthcare Data Platform — PPT Presentation
### 34 Slides | End-to-End Real-Time Streaming & Analytics on AWS

---

## SLIDE 1 — Title Slide

**Title:** Healthcare Data Platform
**Subtitle:** End-to-End Real-Time Streaming, Batch Analytics & AI-Powered Querying on AWS EC2

**Name:** [Your Name]
**Date:** May 2026
**Institution/Organization:** [Your Institution]

**Visuals:** Logos row — Kafka · Flink · MySQL · Airflow · Spark · FastAPI · React · AWS EC2 · Docker · Groq

---

## SLIDE 2 — Problem Statement

**Title:** The Problem with Healthcare Data

**Bullets:**
- Healthcare organizations generate thousands of patient, appointment, and billing events daily
- Data is siloed across departments — no unified real-time view
- Analytics rely on manual exports and delayed batch reports
- Operational teams cannot query data without writing SQL or waiting for IT
- No live visibility into streaming pipeline health or infrastructure status
- Decision-making is slow due to stale, static dashboards

**Speaker Notes:**
Modern hospitals need to track patient flow, revenue, and doctor performance in real time. Legacy systems create bottlenecks — data sits in isolated tables, reports are generated overnight, and there is no conversational interface to ask ad-hoc questions. This platform solves all of these gaps.

---

## SLIDE 3 — Objectives

**Title:** What This Platform Achieves

**Bullets:**
1. Ingest 5 healthcare event types in real-time using Apache Kafka
2. Validate and clean streaming data record-by-record with Apache Flink
3. Persist operational data in MySQL with upsert semantics (idempotent writes)
4. Orchestrate daily batch analytics at 02:00 UTC using Apache Airflow
5. Compute 15 KPI aggregate tables using Apache Spark (PySpark)
6. Visualize all analytics on a 7-page React + FastAPI interactive dashboard
7. Enable natural-language data querying via AI (LLaMA 4 Scout + LangChain + Groq)

**Speaker Notes:**
The platform covers the full data engineering stack — from raw event ingestion to AI-powered querying. Every layer is containerized with Docker and deployed on AWS EC2 for a production-realistic setup.

---

## SLIDE 4 — Tech Stack Overview

**Title:** Technology Stack

**Visual:** Icon grid (3 columns × 4 rows)

| Layer | Technology | Version |
|---|---|---|
| Cloud | AWS EC2 (m7i-flex.large, ap-south-1) | — |
| Messaging | Apache Kafka (Confluent) | 7.4.0 |
| Stream Processing | Apache Flink (PyFlink) | 1.18 |
| Operational DB | MySQL | 8.0 |
| Orchestration | Apache Airflow | 2.9.3 |
| Batch Processing | Apache Spark (PySpark) | 3.5.0 |
| Backend API | FastAPI + Uvicorn | 0.111.0 |
| Frontend | React + Vite + Tailwind CSS | 18 / 5.x |
| AI Model | LLaMA 4 Scout 17B via Groq API | — |
| AI Framework | LangChain | 0.3.x |
| Containerization | Docker + Docker Compose | — |
| Language | Python 3.11, Node.js 18 | — |

**Speaker Notes:**
Every component is open-source or free-tier. Groq provides a free 100K tokens/day API for the LLaMA 4 model, making the AI feature cost-free for demonstration.

---

## SLIDE 5 — Project Scope

**Title:** Scope — What's In and Out

**In Scope:**
- Real-time event streaming and validation (Kafka + Flink)
- Operational data storage with upsert semantics (MySQL)
- Daily batch analytics computation (Airflow + Spark)
- Interactive multi-page dashboard (FastAPI + React)
- AI-powered natural language data querying (LangChain + Groq)
- Infrastructure health monitoring (HTTP + TCP probes)
- Data entry via dashboard forms → Kafka

**Out of Scope:**
- User authentication and authorization
- Multi-region or multi-broker Kafka deployment
- Data encryption at rest or in transit (TLS)
- CI/CD pipeline (GitHub Actions, Jenkins)
- External BI tools (Grafana, Metabase, Tableau)
- Mobile application

**Speaker Notes:**
Scope decisions were made to keep the platform focused on demonstrating data engineering capabilities. Authentication and encryption are noted as natural future enhancements.

---

## SLIDE 6 — High-Level Architecture

**Title:** System Architecture — Two EC2 Instances

**Visual Description:**
```
┌─────────────────────────────────┐          ┌──────────────────────────────────────┐
│  EC21 — 65.0.80.152             │          │  EC22 — 3.6.92.19                    │
│  [Streaming Pipeline]           │          │  [Analytics + Dashboard]              │
│                                 │          │                                      │
│  Faker Producer                 │          │  PostgreSQL (Airflow meta DB)         │
│       ↓                         │          │  Airflow Webserver + Scheduler        │
│  Kafka (5 topics)               │◄─────────│  Spark Master + Worker                │
│       ↓                         │  JDBC    │       ↓ (reads + writes)             │
│  PyFlink (validate)             │  :3308   │  FastAPI Dashboard API               │
│       ↓                         │          │       ↓                              │
│  MySQL :3308                    │  Kafka   │  React Dashboard UI                  │
│  (5 operational tables +        │◄─────────│  (7 pages)                           │
│   15 analytics tables)          │  :9092   │                                      │
└─────────────────────────────────┘          └──────────────────────────────────────┘
```

**Bullets:**
- EC21 handles all real-time ingestion and streaming processing
- EC22 handles batch analytics orchestration and user-facing dashboard
- EC22 reads from and writes back to EC21 MySQL over public IP via JDBC
- EC22 data entry page produces directly to EC21 Kafka via TCP:9092

**Speaker Notes:**
Both instances are in the same AWS region (ap-south-1) to minimize latency. All cross-instance communication uses public IPs since both EC2 instances are in a default VPC without private subnetting.

---

## SLIDE 7 — EC21: Streaming Pipeline Overview

**Title:** EC21 — Real-Time Streaming Pipeline (8 Containers)

**Visual:** Container dependency graph
```
zookeeper (:2181)
    └──► kafka (:9092 / :29092)
              ├──► kafka-ui (:8085)          [monitoring]
              ├──► producer (restart:no)      [data generator]
              └──► flink-jobmanager (:8081)
                        ├──► flink-taskmanager
                        └──► flink-job-submitter (restart:no)
                                    ↓
                              mysql (:3308)
```

**Bullets:**
- Network: `healthcare-net` (Docker bridge)
- Volumes: `mysql-data`, `kafka-data`, `zookeeper-data`, `zookeeper-log`
- Producer: generates data once and exits (`restart: no`)
- Job submitter: auto-discovers and submits all `.py` Flink jobs, then exits
- All other services: `restart: unless-stopped` for high availability

**Speaker Notes:**
The `restart: no` policy for producer and job-submitter ensures they run exactly once — like a one-shot initialization job. All other services restart automatically if they crash.

---

## SLIDE 8 — EC22: Analytics Pipeline Overview

**Title:** EC22 — Batch Analytics + Dashboard (8 Containers)

**Visual:** Container layout
```
postgres (:5432) ──► airflow-init (restart:no)
                          ├──► airflow-webserver (:8080)
                          └──► airflow-scheduler
                                     ↓ (submits jobs)
                  spark-master (:9090/:7077)
                          └──► spark-worker (:8082)
                                     ↓ (JDBC to EC21 MySQL)
                  dashboard-api (:8000)
                          └──► dashboard-ui (:3000)
```

**Bullets:**
- Network: `ec22-net` (Docker bridge)
- Volumes: `postgres-data`, `airflow-logs`
- PostgreSQL stores only Airflow metadata (DAG state, task logs, connections)
- Spark reads EC21 MySQL and writes 15 analytics tables back to it
- Dashboard API reads both operational and analytics tables
- Dashboard UI served by nginx on port 3000

**Speaker Notes:**
The design decision to write analytics tables back to EC21 MySQL (rather than a separate analytics database) simplifies the dashboard — it only needs one MySQL connection string.

---

## SLIDE 9 — EC22: Dashboard Layer

**Title:** Dashboard Architecture — FastAPI + React

**Visual:** Request flow diagram
```
User Browser
    ↓
React (Vite/Tailwind) → nginx (:3000)
    ↓ HTTP / SSE
FastAPI (:8000) — 7 Routers
    ├── /api/financial      → EC21 MySQL (analytics_* tables)
    ├── /api/operational    → EC21 MySQL (analytics_* tables)
    ├── /api/patients       → EC21 MySQL (analytics_* tables)
    ├── /api/pipeline       → Airflow REST API (:8080)
    ├── /api/data-entry     → EC21 Kafka (:9092)
    ├── /api/infrastructure → TCP/HTTP probes (10 services)
    └── /api/chat           → LangChain + Groq API (SSE)
```

**Bullets:**
- CORS: all origins allowed (`allow_origins=["*"]`)
- AI chat uses Server-Sent Events (SSE) for real-time token streaming
- Data entry directly produces to EC21 Kafka topics
- Infrastructure health checks run live probes on every request

---

## SLIDE 10 — AWS Infrastructure

**Title:** AWS Setup — EC2 Instances & Security Groups

**EC2 Configuration:**
| Setting | Value |
|---|---|
| Instance Type | m7i-flex.large (2 vCPU, 8 GB RAM) |
| Region | ap-south-1 (Mumbai) |
| AMI | Amazon Linux 2 / Ubuntu 22.04 LTS |
| Storage | 30 GB gp3 EBS |
| Key Pair | serversfinal.pem |
| Count | 2 instances (EC21, EC22) |

**EC21 Security Group — Inbound Rules:**
| Port | Protocol | Source | Service |
|---|---|---|---|
| 22 | TCP | My IP | SSH |
| 9092 | TCP | 0.0.0.0/0 | Kafka (external) |
| 3308 | TCP | EC22 SG | MySQL |
| 8081 | TCP | 0.0.0.0/0 | Flink UI |
| 8085 | TCP | 0.0.0.0/0 | Kafka UI |
| 2181 | TCP | EC22 SG | Zookeeper |

**EC22 Security Group — Inbound Rules:**
| Port | Protocol | Source | Service |
|---|---|---|---|
| 22 | TCP | My IP | SSH |
| 8080 | TCP | 0.0.0.0/0 | Airflow UI |
| 9090 | TCP | 0.0.0.0/0 | Spark Master UI |
| 8082 | TCP | 0.0.0.0/0 | Spark Worker UI |
| 8000 | TCP | 0.0.0.0/0 | FastAPI |
| 3000 | TCP | 0.0.0.0/0 | React Dashboard |

---

## SLIDE 11 — Docker Networking

**Title:** Container Networking & Volumes

**EC21 — `healthcare-net` (bridge):**
- Internal: `kafka:29092`, `mysql:3306`, `zookeeper:2181`
- External host ports: `9092`, `3308`, `8081`, `8085`, `2181`
- Volumes: `mysql-data` (persistent), `kafka-data`, `zookeeper-data/log`
- Health checks: all critical containers have `healthcheck` blocks with retries

**EC22 — `ec22-net` (bridge):**
- Internal: `postgres:5432`, `airflow-webserver:8080`, `spark-master:7077`
- Cross-EC2 to EC21: `65.0.80.152:3308` (MySQL JDBC), `65.0.80.152:9092` (Kafka)
- Volumes: `postgres-data` (persistent), `airflow-logs`

**Key design patterns:**
- `depends_on` with `condition: service_healthy` ensures startup order
- `x-airflow-common` YAML anchor avoids config duplication across Airflow containers
- All secrets injected via environment variables from `.env` files

---

## SLIDE 12 — Data Producer

**Title:** Faker Data Producer — Event Generation

**Execution Flow:**
```
1. Wait for Kafka (15 retries × 5s = 75s max)
2. Create 5 topics if missing (patients, doctors, appointments, treatments, billing)
3. Send 20 patients (P001–P020) at 50ms intervals → flush
4. Wait 1 second
5. Send 10 doctors (D001–D010) at 50ms intervals → flush
6. Wait 1 second
7. Loop: generate appointment + treatment + billing per event cycle
8. Exit after MAX_EVENTS=100 (configurable)
```

**Data ranges:**
- Patients: 8 insurance providers, DOB 1950–2000, registration 2020–2023
- Doctors: 8 specializations, 5 hospital branches, 1–35 years experience
- Appointments: dates 2023–2024, 8 time slots, 4 visit reasons, 4 statuses
- Treatments: 8 treatment types, cost ₹500–₹8,000
- Billing: 3 payment methods, 3 payment statuses

**Send interval:** 2 seconds between events (configurable via `SEND_INTERVAL`)

---

## SLIDE 13 — Kafka Topics & Schema

**Title:** Kafka — 5 Topics & Message Schema

| Topic | Key | Partitions | Replication |
|---|---|---|---|
| patients | patient_id (P001–P020) | 1 | 1 |
| doctors | doctor_id (D001–D010) | 1 | 1 |
| appointments | appointment_id (A0001+) | 1 | 1 |
| treatments | treatment_id (T0001+) | 1 | 1 |
| billing | bill_id (B0001+) | 1 | 1 |

**Message format:** JSON, UTF-8 encoded, key serialized as string

**Sample appointment message:**
```json
{
  "appointment_id": "A0042",
  "patient_id": "P011",
  "doctor_id": "D003",
  "appointment_date": "2024-03-15",
  "appointment_time": "10:30:00",
  "reason_for_visit": "Consultation",
  "status": "Completed"
}
```

**Kafka configuration:**
- Broker: Confluent 7.4.0
- Listeners: PLAINTEXT (internal :29092), PLAINTEXT_HOST (external :9092)
- `KAFKA_ADVERTISED_LISTENERS`: `65.0.80.152:9092` for external access
- Auto topic creation: disabled (producer creates explicitly via KafkaAdminClient)

---

## SLIDE 14 — PyFlink Validation Pipeline

**Title:** Apache Flink — Validation & Stream Processing

**ValidateAndConvert ProcessFunction (per topic):**
```
Kafka raw string
    ↓
JSON parse  ──► [error] → side-output INVALID_TAG
    ↓
Entity validator ──► [invalid] → side-output INVALID_TAG + warning log
    ↓
Convert dict → typed Flink Row
    ↓
Main output stream → JdbcSink → MySQL
```

**Validation rules per entity:**
| Entity | Key Validation Rules |
|---|---|
| Patient | gender ∈ {M,F}, valid DOB + registration date (YYYY-MM-DD), email regex |
| Doctor | years_experience: 0–60 integer |
| Appointment | valid date + time (HH:MM:SS), status ∈ {Scheduled, Completed, Cancelled, No-show} |
| Treatment | cost > 0, valid treatment_date |
| Billing | amount > 0, payment_method ∈ {Cash, Insurance, Card}, payment_status ∈ {Paid, Pending, Failed} |

**Checkpointing:** every 10 seconds | **Parallelism:** 1 | **Offsets:** earliest (reads all history on start)

---

## SLIDE 15 — MySQL Operational Schema — ERD

**Title:** MySQL — Operational Data Model (5 Tables)

**ERD:**
```
patients (patient_id PK)
    │
    ├──────────────────────────────────────┐
    │                                      │
    ▼                                      │
appointments (appointment_id PK)           │
    │ patient_id FK → patients             │
    │ doctor_id  FK → doctors              │
    │                                      │
    ▼                                      │
treatments (treatment_id PK)              │
    │ appointment_id FK → appointments     │
    │                                      │
    ▼                                      ▼
billing (bill_id PK)
    │ patient_id   FK → patients
    └ treatment_id FK → treatments

doctors (doctor_id PK)
    └── referenced by appointments
```

**Key constraints:**
- All FKs defined but `foreign_key_checks=0` during Flink upserts (allows any arrival order)
- `ON DUPLICATE KEY UPDATE` for idempotent re-processing
- Port 3308 (non-default) to avoid conflicts if MySQL is already installed on host

---

## SLIDE 16 — JDBC Sink: Upsert Strategy

**Title:** Flink → MySQL: Upsert Pattern & Reliability

**JDBC Sink configuration:**
```python
JdbcExecutionOptions:
  - batch_interval_ms: 1000   # flush every 1 second
  - batch_size:        100    # or every 100 rows
  - max_retries:       3      # retry on transient failures

JdbcConnectionOptions:
  - driver: com.mysql.cj.jdbc.Driver
  - useSSL=false
  - allowPublicKeyRetrieval=true
  - serverTimezone=UTC
  - foreign_key_checks=0      # allow out-of-order upserts
```

**Upsert SQL pattern (all 5 tables):**
```sql
INSERT INTO appointments (appointment_id, patient_id, ...)
VALUES (?, ?, ...)
ON DUPLICATE KEY UPDATE
    appointment_date = VALUES(appointment_date),
    status = VALUES(status)
```

**Why upsert?**
- Flink reads from `earliest` offset — re-processes all messages on restart
- Upsert prevents duplicate rows — same `appointment_id` just updates in place
- Guarantees at-least-once delivery with data correctness

---

## SLIDE 17 — Airflow DAG: Flow Diagram

**Title:** Airflow Orchestration — Daily Analytics Pipeline

**DAG Graph:**
```
start
  └──► check_mysql_connection
              └──► init_analytical_schema
                          └──► wait_for_spark_cluster
                                      ├──► financial_analytics    (7 tables)
                                      ├──► operational_analytics  (4 tables)
                                      └──► patient_analytics      (4 tables)
                                                    └──► pipeline_complete
```

**Schedule:** `0 2 * * *` (daily at 02:00 UTC — after streaming pipeline has populated MySQL overnight)

**DAG settings:**
- `catchup: False` — no backfilling of missed runs
- `max_active_runs: 1` — prevents concurrent executions
- `tags: [healthcare, spark, analytics]`
- Default args: `retries: 2`, `retry_delay: 5 minutes`

---

## SLIDE 18 — Airflow DAG: Task Configuration

**Title:** Airflow — Task Breakdown & Operators

| Task | Operator | Description |
|---|---|---|
| start | EmptyOperator | DAG entry point |
| check_mysql_connection | PythonOperator | Connects via pymysql, counts patients table rows |
| init_analytical_schema | PythonOperator | Runs `CREATE TABLE IF NOT EXISTS` for all 15 analytics tables (idempotent) |
| wait_for_spark_cluster | SparkClusterSensor | Polls Spark REST API every 15s, timeout 300s, requires ≥1 worker |
| financial_analytics | SparkSubmitOperator | Submits `financial_analytics.py`, `conn_id=spark_default` |
| operational_analytics | SparkSubmitOperator | Submits `operational_analytics.py` |
| patient_analytics | SparkSubmitOperator | Submits `patient_analytics.py` |
| pipeline_complete | EmptyOperator | Success marker |

**Spark job config:**
- `spark.sql.legacy.timeParserPolicy: LEGACY` (handles various date formats)
- `spark.executor.memory: 1g` | `spark.driver.memory: 1g`
- MySQL JAR: `/opt/spark/jars/mysql-connector-j.jar`

---

## SLIDE 19 — Spark: Financial Analytics (7 Tables)

**Title:** PySpark — Financial Analytics Job

| Analytics Table | Dimensions | Key Metrics |
|---|---|---|
| analytics_revenue_by_doctor | Per doctor | total_bills, total_revenue, avg_bill, max_bill |
| analytics_revenue_by_specialization | Per specialization | doctor_count, appointments, total/avg revenue, avg_per_doc |
| analytics_revenue_by_branch | Per hospital branch | doctor_count, appointments, total/avg revenue |
| analytics_billing_payment | payment_method × payment_status | bill_count, total/avg amount, % of total revenue |
| analytics_outstanding_payments | Pending + Failed only | bill_count, total/avg outstanding, oldest_bill_date |
| analytics_monthly_revenue | Year + Month | bill_count, total/avg revenue, MoM growth % (LAG window) |
| analytics_treatment_cost | Per treatment type | count, avg/min/max/total cost |

**Computation pattern:** JOIN appointments → treatments → billing → doctors
**Window function used:** `Window.orderBy("year","month")` for MoM growth with `F.lag()`
**Write mode:** TRUNCATE + overwrite on each run (idempotent)

---

## SLIDE 20 — Spark: Operational & Patient Analytics (8 Tables)

**Title:** PySpark — Operational & Patient Analytics Jobs

**Operational Analytics (4 tables):**
| Table | Key Metrics |
|---|---|
| analytics_appointment_status | Count per status, completion_rate % = Completed / Total |
| analytics_doctor_workload | appointments_count, unique_patients, avg_cost per doctor |
| analytics_peak_hours | appointment_count per hour-of-day (0–23) |
| analytics_top_doctors_scorecard | Composite score = normalized(revenue + workload + completion_rate) |

**Patient Analytics (4 tables):**
| Table | Key Metrics |
|---|---|
| analytics_patient_spending | total_spend, avg_spend, max_spend per patient |
| analytics_patient_age_groups | patient_count per age bracket (0–18, 19–35, 36–55, 56+) |
| analytics_patient_retention | first_visit, last_visit, visit_count, is_returning flag, retention_rate |
| analytics_new_patient_trend | new_registrations per month (from registration_date) |

**Total: 15 analytics tables across 3 Spark jobs run in parallel by Airflow**

---

## SLIDE 21 — Analytical Schema: Design Rationale

**Title:** Why Pre-Aggregate? Design Decisions Explained

**Problem with on-demand JOINs in the dashboard:**
- A single revenue-by-doctor query requires JOINing 4 tables (appointments, treatments, billing, doctors)
- With 100+ appointments, this is fast — but at 100K+ rows it becomes a bottleneck
- Dashboard would need complex SQL in the API layer

**Solution: Spark pre-aggregates overnight:**
- Dashboard reads `SELECT * FROM analytics_revenue_by_doctor` — single table, sub-100ms
- Spark owns all transformation logic — API stays simple
- Each Spark job is independently testable and deployable

**Why write back to EC21 MySQL?**
- Dashboard needs only ONE MySQL connection string
- No separate analytics database to provision or sync
- 15 analytics tables coexist with 5 operational tables in the `healthcare` DB
- Analytical tables are TRUNCATED on each run — always fresh, never stale

**Idempotency:**
- `init_analytical_schema`: `CREATE TABLE IF NOT EXISTS` — safe to re-run
- Spark write: TRUNCATE + overwrite — same result regardless of how many times run
- Flink upsert: `ON DUPLICATE KEY UPDATE` — replay-safe

---

## SLIDE 22 — Spark Job Architecture

**Title:** Spark Job Design — utils.py Shared Layer

**utils.py — Shared JDBC helpers:**
```python
def build_spark_session(app_name):
    # Configures JDBC driver path, legacy time parser, memory settings

def read_table(spark, table_name):
    # Reads from EC21 MySQL via JDBC, returns DataFrame

def write_table(df, table_name):
    # TRUNCATE + write via JDBC, mode=overwrite, batchsize=1000
```

**Per-job pattern:**
```
1. build_spark_session("HealthcareFinancialAnalytics")
2. setLogLevel("WARN")
3. read_table() for each source table → .cache()
4. Run aggregation functions (one per analytics table)
5. write_table() for each output
6. spark.stop()
```

**Caching strategy:**
- `appointments`, `treatments`, `billing`, `doctors`, `patients` each read once and cached in memory
- Multiple aggregation functions reuse the same cached DataFrame
- Avoids repeated JDBC reads to EC21 MySQL

---

## SLIDE 23 — FastAPI: Router Map

**Title:** FastAPI Backend — 7 Routers & API Design

| Router | Prefix | Key Endpoints | Data Source |
|---|---|---|---|
| financial | `/api/financial` | `/revenue-by-doctor`, `/monthly-revenue`, `/outstanding`, `/billing-payment` | EC21 MySQL analytics_* |
| operational | `/api/operational` | `/appointment-status`, `/peak-hours`, `/doctor-workload`, `/scorecard` | EC21 MySQL analytics_* |
| patients | `/api/patients` | `/spending`, `/age-groups`, `/retention`, `/new-trend` | EC21 MySQL analytics_* |
| pipeline | `/api/pipeline` | `/status` (GET), `/trigger` (POST) | Airflow REST API :8080 |
| data_entry | `/api/data-entry` | `/patient`, `/doctor`, `/appointment`, `/treatment`, `/billing` | EC21 Kafka :9092 |
| infrastructure | `/api/infrastructure` | `/health` | TCP/HTTP probes (10 services) |
| chat | `/api/chat` | `/message` (SSE), `/insights` (sync) | LangChain + Groq API |

**Common patterns:**
- `pymysql` DictCursor for all MySQL reads (returns `List[dict]`)
- `httpx` for Airflow REST calls (sync, timeout=10s)
- `kafka-python` KafkaProducer for data entry writes
- `/health` endpoint returns `{"status": "ok"}` for Docker healthcheck

---

## SLIDE 24 — React Dashboard: All 7 Pages

**Title:** React Dashboard — 7 Interactive Pages

**Financial Page:**
- KPI cards: Total Revenue, Outstanding Payments, Avg Bill Amount, Top Earning Doctor
- Bar chart: Revenue by Specialization
- Line chart: Monthly Revenue Trend with MoM growth %
- Table: Revenue by Doctor (sortable)

**Operational Page:**
- Donut chart: Appointment Status breakdown
- Bar chart: Peak Hours (busiest appointment times)
- Table: Doctor Workload (appointments, patients, avg cost)
- Scorecard: Top Doctors composite ranking

**Patients Page:**
- Pie chart: Patient Age Groups
- KPI cards: Total Patients, Retention Rate, Avg Spend
- Bar chart: New Patient Registrations by Month
- Table: Top Spenders

**Pipeline Page:** DAG status card, task list with state + duration badges, Trigger button

**Infrastructure Page:** 10 service health cards — ✅ online / ❌ offline with response time (ms)

**Data Entry Page:** Forms for each entity type → submits to Kafka on save

**Chat Page:** AI assistant with suggestions, tool chips, markdown responses, session history

---

## SLIDE 25 — Data Entry & Real-Time Write

**Title:** Data Entry — From Dashboard Form to Kafka to MySQL

**End-to-End Flow:**
```
1. User fills form on React DataEntry page
         ↓
2. POST /api/data-entry/{entity} with JSON payload
         ↓
3. FastAPI router validates required fields
         ↓
4. kafka-python KafkaProducer.send(topic, key=id, value=json)
         ↓
5. EC21 Kafka broker receives message on topic
         ↓
6. PyFlink consumer picks up message (reads from latest offset)
         ↓
7. Validate → Convert → JdbcSink → MySQL upsert
         ↓
8. Next dashboard page refresh shows new record
```

**Kafka connection from EC22:**
- Bootstrap server: `65.0.80.152:9092` (EC21 public IP, PLAINTEXT_HOST listener)
- Producer config: `value_serializer=json.dumps + utf-8`, `key_serializer=str.encode`

**Supported entities:** patients, doctors, appointments, treatments, billing
**Latency:** Form submit → MySQL upsert typically completes in 2–4 seconds

---

## SLIDE 26 — Infrastructure Monitoring

**Title:** Live Infrastructure Health — 10 Service Probes

**Probe types:**
- **HTTP probe:** `httpx.get(url, timeout=5s)` → measures response_ms
- **TCP probe:** `socket.create_connection((host, port), timeout=5s)` → measures connect_ms

| # | Service | Probe | Endpoint |
|---|---|---|---|
| 1 | EC21 Kafka broker | TCP | 65.0.80.152:9092 |
| 2 | EC21 MySQL | TCP | 65.0.80.152:3308 |
| 3 | EC21 Zookeeper | TCP | 65.0.80.152:2181 |
| 4 | EC21 Flink UI | HTTP | 65.0.80.152:8081/jobs/overview |
| 5 | EC21 Kafka UI | HTTP | 65.0.80.152:8085 |
| 6 | EC22 Airflow | HTTP | airflow-webserver:8080/health |
| 7 | EC22 Spark Master | HTTP | spark-master:8080/json/ |
| 8 | EC22 Spark Worker | HTTP | spark-worker:8081 |
| 9 | EC22 PostgreSQL | TCP | postgres:5432 |
| 10 | EC22 Dashboard API | HTTP | dashboard-api:8000/health |

**Response format per service:** `{ "status": "online/offline", "response_ms": 42, "error": "..." }`
**All probes run in parallel (dict comprehension) — total check completes in ~5s max**

---

## SLIDE 27 — AI Agent Architecture

**Title:** AI Chat — LangChain + Groq + Manual Tool-Calling Loop

**Architecture Diagram:**
```
User types question
        ↓
React → POST /api/chat/message → asyncio.Queue
        ↓
run_agent() coroutine starts as asyncio.create_task()
        ↓
[SystemMessage + chat_history + HumanMessage]
        ↓
llm.bind_tools(ALL_TOOLS).ainvoke(messages)
        ↓
Groq API → LLaMA 4 Scout 17B
        ↓
    ┌─ response.tool_calls? ──YES──► execute tool(s)
    │                                     ↓
    │                              append ToolMessage
    │                                     ↓
    │                              loop back (max 8 iterations)
    │
    └─ NO ──► return response.content
                    ↓
              SSE event: {type: "token", text: "..."}
                    ↓
              React updates message bubble in real-time
```

**SSE Event Types:**
| Event | Meaning | React Action |
|---|---|---|
| `token` | Final AI response text | Append to message bubble |
| `tool_start` | Tool is executing | Show animated tool chip |
| `tool_end` | Tool finished | Remove tool chip |
| `done` | Response complete | Stop streaming cursor |
| `error` | Something failed | Show error in bubble |

---

## SLIDE 28 — 7 Tools: Capability Map

**Title:** AI Agent Tools — 7 Live System Integrations

| Tool | Icon | System Hit | Example Question |
|---|---|---|---|
| `query_analytics_db` | 🔍 | EC21 MySQL (SELECT only) | "Which doctor earned the most revenue?" |
| `get_pipeline_status` | 🔄 | Airflow REST API | "When did the pipeline last run?" |
| `trigger_analytics_pipeline` | ▶ | Airflow REST API (POST) | "Refresh the analytics data now" |
| `check_infrastructure_health` | 🖥️ | TCP + HTTP probes | "Are all services healthy?" |
| `get_kafka_topic_info` | 📨 | Kafka UI REST API | "How many messages in billing topic?" |
| `get_flink_job_status` | ⚡ | Flink REST API | "Is the streaming pipeline running?" |
| `get_mysql_row_counts` | 🗄️ | EC21 MySQL | "How many records in each table?" |

**Model details:**
- Model: `meta-llama/llama-4-scout-17b-16e-instruct`
- Provider: Groq (free tier — 100K tokens/day, 6K tokens/min)
- Max tokens per response: 2,048 | Temperature: 0 (deterministic)
- SQL safety: only `SELECT` statements allowed in `query_analytics_db`

---

## SLIDE 29 — Manual Tool-Calling Loop: Why & How

**Title:** Why We Use a Manual Loop Instead of LangChain Agent

**The Problem:**
- `create_tool_calling_agent` + Groq free tier + long tool descriptions
- LLaMA generates `<function=name {...}>` format instead of OpenAI JSON
- LangChain agent executor crashes parsing this non-standard format
- `AsyncCallbackHandler` triggers a different Groq code path → `failed_generation` errors

**The Fix — Manual `bind_tools()` loop:**
```python
llm_with_tools = llm.bind_tools(ALL_TOOLS)
messages = [SystemMessage, ...history, HumanMessage]

for _ in range(8):  # max iterations
    response = await llm_with_tools.ainvoke(messages)

    if not response.tool_calls:
        return response.content  # ← final answer

    messages.append(response)  # AIMessage with tool_calls
    for tc in response.tool_calls:
        result = tool.invoke(tc["args"] or {})  # "or {}" fixes args=None crash
        messages.append(ToolMessage(content=str(result), tool_call_id=tc["id"]))
    # loop back with tool results appended
```

**Key patches applied:**
1. `tc["args"] or {}` — fixes crash on no-arg tools (langchain 0.3.30 bug)
2. No `AsyncCallbackHandler` — avoids Groq `failed_generation` on SSE path
3. `GroqAPIError` retry with correction HumanMessage injected

---

## SLIDE 30 — AI Chat Demo

**Title:** AI Chat — Live Demo Walkthrough

**Welcome Screen:**
- 🏥 Healthcare Data Assistant header
- 8 suggestion chips (quick-start questions)
- 7 tool capability cards showing what the AI can do

**Example Interaction 1 — Database Query:**
```
User: "Which doctor earned the most revenue?"
      [🔍 Querying database...]
AI:   "Dr. Jane Smith leads with ₹4,82,350 total revenue from 48 appointments.
       She specializes in Oncology at Central Hospital.

       Top 5 Doctors by Revenue:
       | Doctor          | Specialization | Revenue    |
       |-----------------|---------------|------------|
       | Dr. Jane Smith  | Oncology       | ₹4,82,350  |
       | Dr. John Kumar  | Cardiology     | ₹4,61,200  |
       | ..."
```

**Example Interaction 2 — Infrastructure Check:**
```
User: "Is the Flink streaming job running?"
      [⚡ Checking Flink...]
AI:   "✅ Yes! The Healthcare DataStream Pipeline is RUNNING.
       Uptime: 2h 14m 33s | Job ID: a3f9b2c1...
       Last checkpoint: 8 seconds ago (342ms duration)"
```

**Example Interaction 3 — Multi-tool:**
```
User: "Are all services healthy and when did analytics last run?"
      [🖥️ Checking infrastructure...] [🔄 Checking pipeline...]
AI:   "9/10 services online. EC21 Zookeeper is offline.
       Last DAG run: 2026-05-23 02:00 UTC — state: success (4m 38s)"
```

---

## SLIDE 31 — Functional Requirements

**Title:** Functional Requirements

| ID | Requirement | Category |
|---|---|---|
| FR-01 | System shall ingest events for 5 entity types via Kafka | Ingestion |
| FR-02 | Producer shall auto-create topics if not existing | Ingestion |
| FR-03 | Producer shall send 20 patients and 10 doctors before event loop | Ingestion |
| FR-04 | System shall validate all 5 entity types per defined business rules | Streaming |
| FR-05 | Invalid records shall be routed to side-output logs (not dropped silently) | Streaming |
| FR-06 | Valid records shall be upserted to MySQL using ON DUPLICATE KEY UPDATE | Streaming |
| FR-07 | Flink shall checkpoint state every 10 seconds | Streaming |
| FR-08 | Airflow DAG shall execute daily at 02:00 UTC | Batch |
| FR-09 | DAG shall verify MySQL connectivity and data presence before proceeding | Batch |
| FR-10 | DAG shall idempotently create 15 analytical tables on every run | Batch |
| FR-11 | Financial, Operational, and Patient Spark jobs shall run in parallel | Batch |
| FR-12 | Dashboard shall expose 7 REST API routers with CORS enabled | Dashboard |
| FR-13 | Users shall view 6 analytics pages covering financial, operational, patient, pipeline, infrastructure, and data entry | Dashboard |
| FR-14 | Users shall submit new healthcare records from the dashboard to Kafka | Dashboard |
| FR-15 | AI chat shall answer natural language questions using 7 live tools | AI |
| FR-16 | AI chat responses shall stream to the browser via Server-Sent Events | AI |
| FR-17 | System shall display live health status for all 10 services | Infra |

---

## SLIDE 32 — Non-Functional Requirements

**Title:** Non-Functional Requirements

| Category | ID | Requirement |
|---|---|---|
| Performance | NFR-01 | Kafka message lag < 500ms under normal producer load |
| Performance | NFR-02 | Flink-to-MySQL upsert latency < 2 seconds per batch |
| Performance | NFR-03 | All 3 Spark analytics jobs complete within 10 minutes |
| Performance | NFR-04 | Dashboard API response time < 500ms for analytics reads |
| Performance | NFR-05 | Infrastructure health check completes within 5 seconds |
| Reliability | NFR-06 | Core services auto-restart on failure (restart: unless-stopped) |
| Reliability | NFR-07 | Airflow DAG retries failed tasks 2× with 5-minute delay |
| Reliability | NFR-08 | Producer and job-submitter run exactly once (restart: no) |
| Reliability | NFR-09 | Health checks defined for all critical containers |
| Scalability | NFR-10 | Kafka supports adding partitions without service restart |
| Scalability | NFR-11 | Spark workers can be horizontally scaled via docker compose |
| Security | NFR-12 | All credentials stored in .env files, never hardcoded |
| Security | NFR-13 | .env files excluded from version control via .gitignore |
| Security | NFR-14 | AI chat only allows SELECT SQL statements |
| Maintainability | NFR-15 | Each Spark job is independently executable and testable |
| Maintainability | NFR-16 | Shared Spark utilities extracted to utils.py |

---

## SLIDE 33 — Testing Strategy & Results

**Title:** Testing Approach & Results

**Testing Levels:**

| Level | Scope | Method | Pass Criteria |
|---|---|---|---|
| Unit | Flink validators (5 functions) | Call with valid + each invalid input, assert return value | All edge cases caught |
| Integration | Producer → Kafka → Flink → MySQL | `docker compose up`, send 1 test event, query MySQL | Row appears in correct table |
| System | Full EC21 → EC22 pipeline | Run producer, trigger Airflow DAG, open dashboard | All 15 analytics tables populated, dashboard shows data |
| Manual | Dashboard UI + AI chat | Click through all 7 pages, send 7 AI queries | All pages load, AI returns accurate answers |

**Measured Results:**
| Metric | Result |
|---|---|
| MySQL after producer run | 20 patients, 10 doctors, 100 appointments, 100 treatments, 100 billing rows |
| Flink job state | RUNNING, checkpoints every 10s |
| Airflow DAG duration | 4–6 minutes (all 3 Spark jobs in parallel) |
| Analytics tables populated | All 15 tables with correct aggregations |
| AI tool success rate | 7/7 tools return valid responses |
| Infrastructure probes | 10/10 services detected correctly |

---

## SLIDE 34 — Deployment Steps & Bugs Fixed

**Title:** Deployment Guide & Challenges Solved

**Step-by-Step Deployment:**
```
1. AWS: Launch 2× m7i-flex.large EC2 (ap-south-1), configure security groups
2. SSH EC21: sudo apt install docker.io docker-compose-v2
3. EC21: git clone repo, fill .env (passwords, IPs), docker compose up -d
4. Verify EC21: Kafka UI @ :8085, Flink UI @ :8081, MySQL @ :3308
5. SSH EC22: sudo apt install docker.io docker-compose-v2
6. EC22: fill .env (EC21_IP, Fernet key, Groq API key), docker compose up -d
7. EC22 Airflow UI: create spark_default connection → spark://spark-master:7077
8. Unpause healthcare_analytics_pipeline DAG → trigger first manual run
9. Verify: Spark UI @ :9090, Airflow @ :8080, Dashboard @ :3000
```

**Bugs Encountered & Fixed During Deployment:**

| # | Bug | Root Cause | Fix Applied |
|---|---|---|---|
| 1 | pydantic version conflict | `langchain>=0.3` needs `pydantic>=2.7.4` but `2.7.1` was pinned | Changed to `pydantic>=2.7.4,<3.0.0` |
| 2 | `create_tool_calling_agent` missing | `langchain 1.x` removed it | Pinned `langchain>=0.3.0,<1.0.0` |
| 3 | PEM file chmod fails on NTFS | NTFS mount doesn't support Unix permissions | `cp serversfinal.pem /dev/shm/` first |
| 4 | `args=None` crash on no-arg tools | LangChain 0.3.30 passes `None` for no-arg tools | `tc["args"] or {}` |
| 5 | `service_tier=on_demand` API error | LangChain-Groq sends unsupported param to free tier | Removed from `ChatGroq()` config |
| 6 | LLaMA generates `<function=>` format | Free tier + long descriptions → native LLaMA format | Switched to manual `bind_tools()` loop |
| 7 | `AsyncCallbackHandler` → `failed_generation` | Triggers different Groq streaming code path | Removed callbacks from SSE endpoint |

**Speaker Notes:**
Each bug was traced to the intersection of LangChain version pinning, Groq free-tier limitations, and LLaMA's native tool-call format. The manual loop bypasses all of these issues cleanly.

---

## PRESENTATION DESIGN NOTES

**Color Theme:** Dark navy + teal accent (matches dashboard palette)
- Background: `#0f172a` (navy-950)
- Accent: `#6366f1` (indigo/brand-600)
- Text: White + `#94a3b8` (slate-400)

**Font:** Inter or Roboto — clean, technical

**Slide layout tips:**
- Slides 6, 7, 8, 9, 27: Use diagram/flow visuals — build them in draw.io or Lucidchart
- Slides 15: Use ERD tool (dbdiagram.io) to generate clean diagram
- Slides 17: Export Airflow DAG screenshot from live UI
- Slides 24: Use actual dashboard screenshots
- Slides 30: Use actual AI chat screenshots
- All table slides: dark background tables with subtle borders

**Total: 34 slides**
