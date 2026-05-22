from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "kafka:29092"

TOPICS = [
    "patients",
    "doctors",
    "appointments",
    "treatments",
    "billing",
]

def ensure_topics(bootstrap_servers: str = BOOTSTRAP_SERVERS,
                  num_partitions: int = 1,
                  replication_factor: int = 1) -> None:
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    existing = set(client.list_topics())
    missing  = [t for t in TOPICS if t not in existing]

    if not missing:
        log.info("All topics already exist: %s", TOPICS)
        client.close()
        return

    new_topics = [
        NewTopic(name=t, num_partitions=num_partitions,
                 replication_factor=replication_factor)
        for t in missing
    ]

    try:
        client.create_topics(new_topics=new_topics, validate_only=False)
        log.info("Created topics: %s", [t.name for t in new_topics])
    except TopicAlreadyExistsError:
        log.info("Topics already exist (race condition), skipping.")
    finally:
        client.close()

    # Confirm
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    present = set(client.list_topics())
    client.close()
    for t in TOPICS:
        status = "OK" if t in present else "MISSING"
        log.info("  %-20s %s", t, status)


if __name__ == "__main__":
    ensure_topics()
