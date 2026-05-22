# EC21 Healthcare Pipeline — Connection Reference

## Services Overview

| Service           | Container Name      | Host Port | Internal Port |
|-------------------|---------------------|-----------|---------------|
| MySQL             | mysql               | 3308      | 3306          |
| Kafka Broker      | kafka               | 9092      | 29092         |
| Zookeeper         | zookeeper           | 2181      | 2181          |
| Flink Web UI      | flink-jobmanager    | 8081      | 8081          |
| Kafka UI          | kafka-ui            | 8085      | 8080          |
| Producer          | producer            | —         | —             |
| Flink TaskManager | flink-taskmanager   | —         | —             |

---

## MySQL

| Field    | Value      |
|----------|------------|
| Host     | 127.0.0.1  |
| Port     | 3308       |
| Database | healthcare |
| User     | root       |
| Password | root123    |

**MySQL Workbench:**
- Connection Method: Standard (TCP/IP)
- Hostname: `127.0.0.1`
- Port: `3308`
- Username: `root`
- Password: `root123`
- Default Schema: `healthcare`

**CLI (from host):**
```bash
mysql -h 127.0.0.1 -P 3308 -u root -proot123 healthcare
```

**CLI (from inside container):**
```bash
docker exec -it mysql mysql -uroot -proot123 healthcare
```

**JDBC URL (for Flink / any app):**
```
jdbc:mysql://mysql:3306/healthcare?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC
```

---

## Kafka

| Field                     | Value                |
|---------------------------|----------------------|
| External Bootstrap Server | localhost:9092       |
| Internal Bootstrap Server | kafka:29092          |
| Zookeeper                 | localhost:2181       |
| Topics                    | patients, doctors, appointments, treatments, billing |
| Replication Factor        | 1                    |
| Partitions per Topic      | 1                    |

**List topics:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

**Consume a topic (e.g. patients):**
```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic patients \
  --from-beginning
```

---

## Flink

| Field           | Value                    |
|-----------------|--------------------------|
| Web UI          | http://localhost:8081    |
| JobManager RPC  | flink-jobmanager:6123    |
| REST API        | http://localhost:8081/v1 |
| Parallelism     | 2                        |
| Task Slots      | 4                        |

**Submit a PyFlink job:**
```bash
docker exec flink-jobmanager flink run -py /opt/flink/jobs/healthcare_job.py
```

**Check running jobs:**
```bash
curl http://localhost:8081/v1/jobs
```

---

## Kafka UI

| Field    | Value                  |
|----------|------------------------|
| URL      | http://localhost:8085  |
| Cluster  | healthcare-cluster     |

---

## Producer

| Field          | Value         |
|----------------|---------------|
| Send Interval  | 2.0 seconds   |
| Max Events     | 100 (0 = unlimited) |
| Initial Patients | 20          |
| Initial Doctors  | 10          |

**Environment variables (docker-compose.yml):**
```yaml
KAFKA_BOOTSTRAP_SERVERS: kafka:29092
SEND_INTERVAL: "2.0"
MAX_EVENTS: "100"
```

**View live logs:**
```bash
docker logs -f producer
```

---

## Docker Network

| Field         | Value            |
|---------------|------------------|
| Network Name  | ec21_healthcare-net |
| Driver        | bridge           |

All containers communicate using their **container names** as hostnames within this network.

---

## Quick Start

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# Rebuild and restart a specific service
docker compose up --build -d <service-name>

# View logs for a service
docker logs -f <container-name>
```
