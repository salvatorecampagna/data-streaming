"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

TOPIC_PREFIX = "org.chicago.cta.station.turnstiles"
TOPIC_VERSION = "v1"
FORMAT = "avro"
KEY = "station_id"


KSQL_STATEMENT = f"""
CREATE TABLE turnstile (
    station_id INTEGER,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='{TOPIC_PREFIX}.{TOPIC_VERSION}',
    VALUE_FORMAT='{FORMAT}',
    KEY='{KEY}'
);

CREATE TABLE turnstile_summary
AS
    SELECT
        station_id, COUNT(*) as count
    FROM
        turnstile
    GROUP BY
        station_id
;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement\n%s", KSQL_STATEMENT)
    print(KSQL_STATEMENT)

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
