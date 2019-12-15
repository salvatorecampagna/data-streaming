"""
Configures a Kafka Connector for Postgres Station data
"""

import json
import logging
import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
DB = "cta"
DB_USER = "cta_admin"
DB_PASS = "chicago"
DB_TABLES = "stations"
MODE = "incrementing"
INCREMENTING_COLUMN_NAME = "stop_id"
TOPIC_PREFIX = "org.chicago.cta."
POLL_INTERVAL_MS = "1800000"


def configure_connector():
    """
    Starts and configures the Kafka Connect connector
    """

    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "batch.max.rows": "500",
        "connection.url": f"jdbc:postgresql://postgres:5432/{DB}",
        "connection.user": f"{DB_USER}",
        "connection.password": f"{DB_PASS}",
        "table.whitelist": f"{DB_TABLES}",
        "mode": f"{MODE}",
        "incrementing.column.name": f"{INCREMENTING_COLUMN_NAME}",
        "topic.prefix": f"{TOPIC_PREFIX}",
        "poll.interval.ms": f"{POLL_INTERVAL_MS}",
    }

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": config
            }
        ),
    )

    try:
        resp.raise_for_status()
    except e:
        logger.error(
            "error while creating connector '%s'",
            json.dumps(resp.json(), indent=2)
        )
        raise

    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
