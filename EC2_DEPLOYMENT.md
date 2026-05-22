# EC2 Deployment Guide — Healthcare Streaming Pipeline

---

## 1. Recommended EC2 Instance

| Setting        | Recommendation                              |
|----------------|---------------------------------------------|
| Instance type  | `m7i-flex.xlarge` (4 vCPU, 16 GB RAM) — see table below |
| OS / AMI       | Amazon Linux 2023 or Ubuntu 22.04 LTS       |
| Storage (EBS)  | 30 GB gp3 root volume                       |
| Key pair       | Create/select a key pair for SSH            |

### Instance Sizing Options

| Instance           | vCPU | RAM   | On-Demand/hr | Use Case                                  |
|--------------------|------|-------|--------------|-------------------------------------------|
| `m7i-flex.large`   | 2    | 8 GB  | ~$0.096      | Light testing only (tight on memory)      |
| `m7i-flex.xlarge`  | 4    | 16 GB | ~$0.190      | **Recommended — dev/test and production** |
| `m7i-flex.2xlarge` | 8    | 32 GB | ~$0.381      | High-throughput / heavy load              |
| `t3.xlarge`        | 4    | 16 GB | ~$0.166      | Alternative (burstable, older gen)        |

**Why `m7i-flex.xlarge` over `t3.xlarge`:**
- Newer Intel Xeon (Ice Lake/Sapphire Rapids) — better sustained CPU performance
- No CPU credit bursting — consistent performance under load
- Better network bandwidth (up to 12.5 Gbps vs 5 Gbps on t3)
- Similar price point

**On `m7i-flex.large` (8 GB):** Flink + Kafka + MySQL idle together use ~6–8 GB RAM.  
You may see OOM kills under load or during Flink checkpointing. Only use it for quick tests with `MAX_EVENTS=100` and `FLINK_PARALLELISM=1` in `.env`.

> For production workloads use `m7i-flex.xlarge` or larger.

---

## 2. Security Group — Inbound Rules

Open these ports in the EC2 Security Group:

| Port  | Protocol | Source            | Purpose                  |
|-------|----------|-------------------|--------------------------|
| 22    | TCP      | Your IP only      | SSH access               |
| 8081  | TCP      | Your IP only      | Flink Web UI             |
| 8085  | TCP      | Your IP only      | Kafka UI                 |
| 3308  | TCP      | Your IP only      | MySQL (optional/workbench)|
| 9092  | TCP      | Your IP only      | Kafka (optional/external) |
| 2181  | TCP      | Your IP only      | Zookeeper (optional)      |

> **Security tip:** Restrict all ports to your own IP (`My IP` in AWS console), not `0.0.0.0/0`.

---

## 3. Connect to EC2

```bash
ssh -i /path/to/your-key.pem ec2-user@<EC2_PUBLIC_IP>
# Ubuntu AMI: use ubuntu@ instead of ec2-user@
```

---

## 4. Install Docker & Docker Compose

### Amazon Linux 2023
```bash
sudo dnf update -y
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker ec2-user
newgrp docker

# Docker Compose plugin
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

docker compose version   # verify
```

### Ubuntu 22.04
```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker ubuntu
newgrp docker

docker compose version   # verify
```

---

## 5. Transfer Project Files to EC2

### Option A — SCP (from your local machine)
```bash
scp -i /path/to/your-key.pem -r /path/to/EC21 ec2-user@<EC2_PUBLIC_IP>:~/EC21
```

### Option B — Git (if repo is on GitHub/GitLab)
```bash
git clone https://github.com/your-org/EC21.git ~/EC21
```

### Option C — rsync (faster for large folders)
```bash
rsync -avz --progress -e "ssh -i /path/to/your-key.pem" \
  /path/to/EC21/ ec2-user@<EC2_PUBLIC_IP>:~/EC21/
```

---

## 6. Configure `.env` for EC2

After copying the files, update one variable in `.env` on the EC2 instance:

```bash
nano ~/EC21/.env
```

Change `FLINK_REST_URL` from `localhost` to the container-internal address:

```env
FLINK_REST_URL=http://flink-jobmanager:8081
```

> This is needed because `submit_job.py` runs **inside** the Flink container, not on localhost.  
> All other `.env` values remain the same.

---

## 7. Start the Pipeline

```bash
cd ~/EC21

# Build images and start all services
docker compose up -d --build

# Watch startup progress
docker compose ps

# Tail logs
docker compose logs -f
```

Startup order (automatic via healthchecks):
1. Zookeeper → 2. Kafka → 3. MySQL → 4. Flink JobManager → 5. Flink TaskManager → 6. Producer + Kafka UI

---

## 8. Submit Flink Jobs

Once all containers are healthy (takes ~2–3 minutes):

```bash
cd ~/EC21
bash flink/submit_jobs.sh
```

---

## 9. Access Web UIs

Replace `<EC2_PUBLIC_IP>` with your instance's public IP or Elastic IP:

| UI        | URL                              |
|-----------|----------------------------------|
| Flink     | `http://<EC2_PUBLIC_IP>:8081`    |
| Kafka UI  | `http://<EC2_PUBLIC_IP>:8085`    |

---

## 10. Verify Data in MySQL

```bash
docker exec mysql mysql -uroot -proot123 -e "
  SELECT 'patients'     AS tbl, COUNT(*) AS rows FROM healthcare.patients     UNION ALL
  SELECT 'doctors',              COUNT(*)         FROM healthcare.doctors      UNION ALL
  SELECT 'appointments',         COUNT(*)         FROM healthcare.appointments UNION ALL
  SELECT 'treatments',           COUNT(*)         FROM healthcare.treatments   UNION ALL
  SELECT 'billing',              COUNT(*)         FROM healthcare.billing;
"
```

---

## 11. Restart Producer for More Events

The producer stops after `MAX_EVENTS=100` events by default:

```bash
docker start producer
```

To change the event count, edit `.env` and recreate:

```bash
# In .env: MAX_EVENTS=500
docker compose up -d producer
```

---

## 12. Stop Everything

```bash
# Stop containers (data volumes preserved)
docker compose down

# Stop AND delete all data volumes (clean slate)
docker compose down -v
```

---

## 13. Auto-Start on EC2 Reboot (optional)

To start the pipeline automatically when the EC2 instance reboots:

```bash
# Create a systemd service
sudo tee /etc/systemd/system/healthcare-pipeline.service > /dev/null <<EOF
[Unit]
Description=Healthcare Streaming Pipeline
After=docker.service
Requires=docker.service

[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/home/ec2-user/EC21
ExecStart=/usr/local/bin/docker compose up -d
ExecStop=/usr/local/bin/docker compose down
User=ec2-user

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl enable healthcare-pipeline
```

---

## 14. Cost Estimate (AWS)

| Resource                   | Type              | Approx. Cost/month |
|----------------------------|-------------------|--------------------|
| `m7i-flex.large` (8 GB)    | On-Demand         | ~$70               |
| `m7i-flex.large` (8 GB)    | Spot Instance     | ~$21 (70% savings) |
| `m7i-flex.xlarge` (16 GB)  | On-Demand         | ~$138              |
| `m7i-flex.xlarge` (16 GB)  | Spot Instance     | ~$41 (70% savings) |
| `m7i-flex.2xlarge` (32 GB) | On-Demand         | ~$277              |
| `m7i-flex.2xlarge` (32 GB) | Spot Instance     | ~$83 (70% savings) |
| EBS 30 GB gp3              | Storage           | ~$2.40             |
| Data transfer out          | First 100 GB/mo   | Free               |

> - Use **Spot Instances** for dev/test — up to 70% cheaper, can be interrupted with 2-min notice  
> - Use **On-Demand** for production or long-running jobs  
> - Use **Reserved Instance (1-year)** for ~40% savings on stable production workloads  
> - Stop the instance when not in use — you only pay for EBS storage while stopped (~$2.40/mo)

---

## Quick Reference

```bash
# SSH in
ssh -i key.pem ec2-user@<EC2_PUBLIC_IP>

# Start pipeline
cd ~/EC21 && docker compose up -d

# Submit jobs
bash flink/submit_jobs.sh

# Check status
docker compose ps

# Restart producer
docker start producer

# Stop pipeline
docker compose down
```
