"""Spark Structured Streaming transformations for Layer 3 (Speed Layer)."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from speed.lib.schemas import ENVELOPE_MAJOR_VERSION, STATION_STATUS_ENVELOPE_SCHEMA

VALID_WATERMARK_MODES = ("last_reported", "snapshot_ts")


def parse_station_status_envelope(raw_df: DataFrame) -> DataFrame:
    """Parse Envelope-wrapped StationStatus JSON messages from Kafka into flat UTC columns."""
    select_exprs = [F.from_json(F.col("value").cast("string"), STATION_STATUS_ENVELOPE_SCHEMA).alias("env")]
    has_kafka_meta = {"partition", "offset"}.issubset(raw_df.columns)
    if has_kafka_meta:
        select_exprs.extend([
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        ])

    parsed = raw_df.select(*select_exprs)

    valid = parsed.filter(
        F.col("env").isNotNull()
        & (F.split(F.col("env.schema_version"), r"\.")[0] == F.lit(ENVELOPE_MAJOR_VERSION))
        & (F.col("env.record_type") == F.lit("station_status"))
        & F.col("env.payload.station_id").isNotNull()
    )

    cols = [
        F.col("env.schema_version").alias("schema_version"),
        F.col("env.record_type").alias("record_type"),
        F.col("env.payload.system_id").alias("system_id"),
        F.col("env.payload.station_id").alias("station_id"),
        F.to_timestamp(F.col("env.payload.snapshot_ts")).alias("snapshot_ts"),
        F.to_timestamp(F.col("env.payload.last_reported")).alias("last_reported"),
        F.to_timestamp(F.col("env.payload.ingest_ts")).alias("ingest_ts"),
        F.to_timestamp(F.col("env.fetched_at")).alias("fetched_at"),
        F.col("env.payload.num_bikes_available").alias("num_bikes_available"),
        F.col("env.payload.num_bikes_disabled").alias("num_bikes_disabled"),
        F.col("env.payload.num_docks_available").alias("num_docks_available"),
        F.col("env.payload.num_docks_disabled").alias("num_docks_disabled"),
        F.col("env.payload.num_ebikes_available").alias("num_ebikes_available"),
        F.col("env.payload.is_installed").alias("is_installed"),
        F.col("env.payload.is_renting").alias("is_renting"),
        F.col("env.payload.is_returning").alias("is_returning"),
        F.col("env.payload.vehicle_types_available").alias("vehicle_types_available"),
        F.col("env.payload.extra").alias("extra"),
        F.col("env.payload.source_version").alias("source_version"),
    ]
    if has_kafka_meta:
        cols.extend([F.col("kafka_partition"), F.col("kafka_offset")])

    return valid.select(*cols)


def dedup_and_watermark_station_status(
    parsed_df: DataFrame,
    watermark_delay: str = "10 minutes",
    watermark_mode: str = "last_reported",
) -> DataFrame:
    """Apply watermark for late-data handling and deduplicate by (system_id, station_id, last_reported).

    When ``last_reported`` is null, ``snapshot_ts`` is used as fallback event timestamp
    so records still advance and respect the streaming watermark.
    """
    if watermark_mode not in VALID_WATERMARK_MODES:
        raise ValueError(
            f"Unsupported watermark_mode={watermark_mode!r}. Expected one of {VALID_WATERMARK_MODES}."
        )

    # Coalesce handles stations with null last_reported (observed in Citi Bike & Bixi feeds)
    with_event_ts = parsed_df.withColumn(
        "event_ts",
        F.coalesce(F.col("last_reported"), F.col("snapshot_ts")),
    )

    if watermark_mode == "last_reported":
        return (
            with_event_ts
            .withWatermark("event_ts", watermark_delay)
            .dropDuplicates(["system_id", "station_id", "event_ts"])
        )

    return (
        with_event_ts
        .withWatermark("snapshot_ts", watermark_delay)
        .dropDuplicatesWithinWatermark(["system_id", "station_id", "event_ts"])
    )
