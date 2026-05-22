import os

MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3308"))
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "healthcare")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root123")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "65.0.80.152:9092")

AIRFLOW_API_URL  = os.getenv("AIRFLOW_API_URL",  "http://airflow-webserver:8080")
AIRFLOW_USER     = os.getenv("AIRFLOW_USER",     "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin123")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

DAG_ID = "healthcare_analytics_pipeline"
