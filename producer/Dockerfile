FROM python:3.10-slim

RUN pip install --no-cache-dir kafka-python==2.0.2 faker==19.13.0

WORKDIR /app
COPY producer.py .

CMD ["python", "producer.py"]
