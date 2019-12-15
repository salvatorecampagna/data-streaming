"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

IN_TOPIC_PREFIX = "org.chicago.cta.stations.table"
OUT_TOPIC_PREFIX = "org.chicago.cta.stations.transformed"
TOPIC_VERSION = "v1"

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic(f"{IN_TOPIC_PREFIX}.{TOPIC_VERSION}", value_type=Station)
out_topic = app.topic(f"{OUT_TOPIC_PREFIX}.{TOPIC_VERSION}", partitions=1)
table = app.Table(
    "stations",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def process_stations(stations):
    async for station in stations:

        color = "green"
        if station.red:
            line="red"
        elif station.blue:
            line="blue"

        transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=color
        )

        table[station.station_id] = transformed_station


if __name__ == "__main__":
    app.main()
