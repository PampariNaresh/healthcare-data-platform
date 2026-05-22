# EC21 Healthcare Streaming Pipeline

Real-time data pipeline using Apache Kafka, Apache Flink (PyFlink), and MySQL.
Generates synthetic healthcare events, validates and transforms them in Flink, and persists to MySQL.
MySQL data is consumed downstream by EC22 for batch analytics.

---

## Architecture

```
Producer → Kafka Topics → Flink DataStream (validate + transform) → MySQL
                                    ↓ invalid records
                               Side Output → Flink task logs
```

| Layer         | Technology                  | Purpose                                         |
|---------------|-----------------------------|-------------------------------------------------|
| Ingestion     | Python Producer (Faker)     | Generates random healthcare events              |
| Messaging     | Apache Kafka 7.4.0          | Streams events per topic                        |
| Coordination  | Zookeeper 7.4.0             | Kafka broker coordination                       |
| Processing    | Apache Flink 1.18 (PyFlink) | Validates, transforms and writes to MySQL       |
| Storage       | MySQL 8.0                   | Persists all validated healthcare records       |
| Monitoring    | Kafka UI, Flink Web UI      | Observe topics, consumer groups and Flink jobs  |
| Downstream    | EC22 (Airflow + Spark)      | Batch analytics on top of MySQL data            |

---

## Project Structure

```
EC21/
├── .env                            # All environment variables (single source of truth)
├── docker-compose.yml              # All service definitions
├── README.md                       # This file
│
├── mysql/
│   └── init.sql                    # Healthcare schema — 5 operational tables
│
├── kafka/
│   └── Dockerfile                  # cp-kafka:7.4.0 + which utility
│
├── producer/
│   ├── Dockerfile                  # Python 3.10 + kafka-python + faker
│   └── producer.py                 # Synthetic event generator → 5 Kafka topics
│
├── flink/
│   ├── Dockerfile                  # PyFlink 1.18 + Kafka/JDBC/MySQL connector JARs
│   ├── submit_jobs.sh              # Shell script to submit jobs via docker exec
│   └── jobs/
│       ├── healthcare_job.py       # Main DataStream job: Kafka → validate → MySQL
│       ├── submit_job.py           # Multi-job submitter with retry + exponential backoff
│       └── kafka_topics_setup.py   # Topic existence check / create utility
│
└── data/                           # Sample CSV files (reference / seed data)
    ├── patients.csv
    ├── doctors.csv
    ├── appointments.csv
    ├── treatments.csv
    └── billing.csv
```

---

## Services & Ports

| Service           | Container          | Host Port | Purpose                        |
|-------------------|--------------------|-----------|--------------------------------|
| MySQL             | mysql              | 3308      | Healthcare operational database |
| Kafka Broker      | kafka              | 9092      | Message broker (external)      |
| Zookeeper         | zookeeper          | 2181      | Kafka coordination             |
| Flink JobManager  | flink-jobmanager   | 8081      | Flink Web UI & REST API        |
| Flink TaskManager | flink-taskmanager  | —         | Job execution (4 slots)        |
| Kafka UI          | kafka-ui           | 8085      | Kafka browser UI               |
| Producer          | producer           | —         | Event generator (runs once)    |

---

## Credentials

| Service | User  | Password |
|---------|-------|----------|
| MySQL   | root  | root123  |

All values are defined in `.env` and referenced by all services.

---

## Kafka Topics

Each topic maps directly to a MySQL table. Flink reads from all 5 topics in parallel.

| Topic        | MySQL Table  | Key        | Event Rate              |
|--------------|--------------|------------|-------------------------|
| patients     | patients     | patient_id | Initial batch of 20     |
| doctors      | doctors      | doctor_id  | Initial batch of 10     |
| appointments | appointments | appt_id    | Continuous (every N sec)|
| treatments   | treatments   | treatment_id | Continuous            |
| billing      | billing      | bill_id    | Continuous              |

---

## MySQL Schema (healthcare)

### Operational Tables

```
patients
  patient_id (PK), first_name, last_name, gender, date_of_birth,
  contact_number, address, registration_date, insurance_provider,
  insurance_number, email

doctors
  doctor_id (PK), first_name, last_name, specialization, phone_number,
  years_experience, hospital_branch, email

appointments
  appointment_id (PK), patient_id (FK), doctor_id (FK),
  appointment_date, appointment_time, reason_for_visit, status

treatments
  treatment_id (PK), appointment_id (FK),
  treatment_type, description, cost, treatment_date

billing
  bill_id (PK), patient_id (FK), treatment_id (FK),
  bill_date, amount, payment_method, payment_status
```

All tables use `ON DUPLICATE KEY UPDATE` — safe to replay Kafka topics from earliest offset.

---

## Producer — producer.py

Sends events in two phases:

**Phase 1 — Initial batch (runs once at startup):**
- 20 patients → `patients` topic
- 10 doctors  → `doctors` topic

**Phase 2 — Continuous loop:**
- Every `SEND_INTERVAL` seconds, generates one linked set:
  `appointment → treatment → billing`
- Stops at `MAX_EVENTS` (0 = unlimited)

**Generated data ranges:**

| Field              | Range / Values                                              |
|--------------------|-------------------------------------------------------------|
| specializations    | Cardiology, Dermatology, Pediatrics, Orthopedics, Neurology, Oncology, Gastroenterology, Psychiatry |
| hospital branches  | Central Hospital, Westside Clinic, Eastside Clinic, North Wing, South Wing |
| insurance providers| HealthIndia, WellnessCorp, PulseSecure, MediShield, CarePlus |
| appointment status | Scheduled, Completed, Cancelled, No-show                   |
| treatment types    | MRI, CT Scan, Chemotherapy, Surgery, Physiotherapy, X-Ray, Blood Test, ECG |
| treatment cost     | ₹500 – ₹8,000 (random float)                               |
| payment methods    | Cash, Insurance, Card                                       |
| payment statuses   | Paid, Pending, Failed                                       |

---

## Flink Pipeline — healthcare_job.py

One pipeline per Kafka topic, all running in parallel:

```
KafkaSource (topic)
    └─► ValidateAndConvert (ProcessFunction)
              ├─► valid records   → JdbcSink (MySQL, batch=100, interval=1s)
              └─► invalid records → Side Output → Flink task logs
```

**Validations performed:**

| Topic        | Validations                                                         |
|--------------|---------------------------------------------------------------------|
| patients     | Required fields, gender ∈ {M, F}, valid dates, email format         |
| doctors      | Required fields, years_experience in 0–60                          |
| appointments | Required fields, valid date/time, status ∈ allowed set             |
| treatments   | Required fields, cost > 0, valid treatment_date                    |
| billing      | Required fields, amount > 0, payment_method and status ∈ allowed sets |

Invalid records are routed to a **side output** and printed to Flink task logs — they are never written to MySQL.

**JDBC sink settings:** batch size = 100 records, flush interval = 1s, max retries = 3.

---

## Job Submission — submit_job.py

Multi-threaded job submitter with automatic retry:

- Submits all jobs listed in `JOB_SCRIPTS` env var (comma-separated)
- Each job runs in its own thread
- On `FAILED` or `CANCELED`: retries with **exponential backoff** (initial delay doubles each attempt, capped at 5 min)
- On `FINISHED`: thread exits cleanly
- Exits non-zero if any job exhausts all retries

---

## Environment Variables (.env)

| Variable                    | Default                             | Description                         |
|-----------------------------|-------------------------------------|-------------------------------------|
| `MYSQL_ROOT_PASSWORD`       | root123                             | MySQL root password                 |
| `MYSQL_DATABASE`            | healthcare                          | Database name                       |
| `MYSQL_USER`                | root                                | App-level DB user (Flink/producer)  |
| `MYSQL_PASSWORD`            | root123                             | App-level DB password               |
| `MYSQL_HOST_PORT`           | 3308                                | MySQL port exposed on host          |
| `MYSQL_CONTAINER_PORT`      | 3306                                | MySQL port inside container         |
| `KAFKA_HOST_PORT`           | 9092                                | Kafka external port                 |
| `KAFKA_INTERNAL_PORT`       | 29092                               | Kafka internal port (inter-service) |
| `KAFKA_BROKER_ID`           | 1                                   | Kafka broker ID                     |
| `KAFKA_REPLICATION_FACTOR`  | 1                                   | Topic replication factor            |
| `KAFKA_AUTO_CREATE_TOPICS`  | true                                | Auto-create topics on first publish |
| `KAFKA_UI_PORT`             | 8085                                | Kafka UI port                       |
| `ZOOKEEPER_PORT`            | 2181                                | Zookeeper client port               |
| `FLINK_UI_PORT`             | 8081                                | Flink Web UI & REST port            |
| `FLINK_TASK_SLOTS`          | 4                                   | Task slots per TaskManager          |
| `FLINK_PARALLELISM`         | 2                                   | Default job parallelism             |
| `FLINK_CHECKPOINT_INTERVAL` | 10000                               | Checkpoint interval (ms)           |
| `KAFKA_BOOTSTRAP_SERVERS`   | kafka:29092                         | Bootstrap servers (internal)        |
| `SEND_INTERVAL`             | 2.0                                 | Seconds between producer events     |
| `MAX_EVENTS`                | 100                                 | Max events from producer (0=unlimited) |
| `FLINK_REST_URL`            | http://localhost:8081               | Flink REST API URL                  |
| `MAX_RETRIES`               | 5                                   | Max job resubmit attempts           |
| `RETRY_DELAY`               | 30                                  | Initial retry delay (seconds)       |
| `POLL_INTERVAL`             | 10                                  | Job status poll interval (seconds)  |
| `JOB_SCRIPTS`               | /opt/flink/jobs/healthcare_job.py   | Comma-separated job scripts to run  |

---

## Quick Start

```bash
# 1. Start all services
docker compose up -d

# 2. Verify all containers are healthy
docker compose ps

# 3. Submit Flink jobs
./flink/submit_jobs.sh

# 4. Generate more events (re-run the producer)
docker start producer

# 5. Stop everything
docker compose down

# Stop and remove volumes (full reset)
docker compose down -v
```

> **Note:** The producer runs once and stops (`restart: "no"`).
> Re-run with `docker start producer` to generate another batch.

---

## Starting Only MySQL (for EC22 local testing)

EC22 needs only MySQL from this stack. Start just that service:

```bash
docker compose up mysql -d
```

Then apply EC22's analytical schema:
```bash
docker exec mysql mysql -uroot -proot123 healthcare \
  < ../EC22/mysql/analytical_schema.sql
```

---

## Adding a New Flink Job

1. Create your job script in `flink/jobs/`
2. Register it by appending to `JOB_SCRIPTS` in `.env`:
   ```env
   JOB_SCRIPTS=/opt/flink/jobs/healthcare_job.py,/opt/flink/jobs/your_job.py
   ```
3. Submit: `./flink/submit_jobs.sh`

Or pass it as a one-off override without changing `.env`:
```bash
./flink/submit_jobs.sh JOB_SCRIPTS=/opt/flink/jobs/your_job.py
```

---

## Useful Commands

```bash
# Stream producer logs
docker logs -f producer

# Stream Flink JobManager logs
docker logs -f flink-jobmanager

# List Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume a topic from the beginning
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic patients --from-beginning

# Check Kafka consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --list

# Query MySQL record counts
docker exec mysql mysql -uroot -proot123 -e "
  SELECT 'patients'     AS tbl, COUNT(*) AS cnt FROM healthcare.patients     UNION ALL
  SELECT 'doctors'      AS tbl, COUNT(*) AS cnt FROM healthcare.doctors      UNION ALL
  SELECT 'appointments' AS tbl, COUNT(*) AS cnt FROM healthcare.appointments UNION ALL
  SELECT 'treatments'   AS tbl, COUNT(*) AS cnt FROM healthcare.treatments   UNION ALL
  SELECT 'billing'      AS tbl, COUNT(*) AS cnt FROM healthcare.billing;
"

# Check Flink running jobs via REST
curl http://localhost:8081/v1/jobs

# Submit jobs with overrides (no .env change needed)
./flink/submit_jobs.sh MAX_RETRIES=3 POLL_INTERVAL=5

# Rebuild images after code changes
docker compose up -d --build flink-jobmanager flink-taskmanager producer
```

---

## Web UIs

| UI          | URL                   | Purpose                          |
|-------------|-----------------------|----------------------------------|
| Flink       | http://localhost:8081 | Job overview, task managers, logs |
| Kafka UI    | http://localhost:8085 | Browse topics, messages, groups  |
