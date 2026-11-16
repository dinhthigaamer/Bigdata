"""Prototype Job A: Read Kafka gbfs.station_status with Spark Structured Streaming,
parse Envelope schema, apply watermark/late-data filtering, deduplicate by
(system_id, station_id, last_reported), and print output to console.
"""
import argparse
import glob
import logging
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from common.envelope import StationStatusEnvelope
from speed.lib.transforms import (
    VALID_WATERMARK_MODES,
    dedup_and_watermark_station_status,
    parse_station_status_envelope,
)

SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
PRELOADED_JARS_DIR = "/opt/spark-jars"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)
logging.getLogger("py4j.java_gateway").setLevel(logging.WARNING)
log = logging.getLogger("speed.job_a_console")


class ConsoleBatchMetricsListener(StreamingQueryListener):
    """Logs micro-batch input, watermark, state, and late-drop counters."""

    def onQueryStarted(self, event) -> None:
        log.info("Streaming query started: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event) -> None:
        p = event.progress
        watermark = p.eventTime.get("watermark", "N/A") if p.eventTime else "N/A"
        dropped_late = sum(op.numRowsDroppedByWatermark for op in p.stateOperators)
        state_rows = sum(op.numRowsTotal for op in p.stateOperators)
        state_updated = sum(op.numRowsUpdated for op in p.stateOperators)
        output_rows = p.sink.numOutputRows if p.sink else 0
        log.info(
            "[Batch #%d] input=%d | output=%d | state_total=%d | state_updated=%d | dropped_late=%d | watermark=%s",
            p.batchId,
            p.numInputRows,
            output_rows,
            state_rows,
            state_updated,
            dropped_late,
            watermark,
        )

    def onQueryTerminated(self, event) -> None:
        if event.exception:
            log.error("Streaming query terminated with error: %s", event.exception)
        else:
            log.info("Streaming query terminated cleanly: id=%s", event.id)


def build_spark_session(app_name: str = "job-a-console", shuffle_partitions: int = 4) -> SparkSession:
    """Create a UTC SparkSession configured with the Kafka SQL connector."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    )
    jar_files = sorted(glob.glob(os.path.join(PRELOADED_JARS_DIR, "*.jar")))
    if jar_files:
        builder = builder.config("spark.jars", ",".join(jar_files))
    else:
        builder = builder.config("spark.jars.packages", SPARK_KAFKA_PACKAGE)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run(args: argparse.Namespace) -> None:
    spark = build_spark_session(shuffle_partitions=args.shuffle_partitions)
    spark.streams.addListener(ConsoleBatchMetricsListener())

    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if args.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", str(args.max_offsets_per_trigger))

    raw_df = reader.load()
    parsed_df = parse_station_status_envelope(raw_df)
    deduped_df = dedup_and_watermark_station_status(
        parsed_df,
        watermark_delay=args.watermark,
        watermark_mode=args.watermark_mode,
    )

    display_df = deduped_df.select(
        "system_id",
        "station_id",
        "last_reported",
        "snapshot_ts",
        "event_ts",
        "num_bikes_available",
        "num_ebikes_available",
        "num_docks_available",
        "is_renting",
        "is_returning",
        "kafka_partition",
        "kafka_offset",
    )

    writer = (
        display_df.writeStream
        .queryName("job_a_station_status_console")
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", str(args.num_rows))
        .option("checkpointLocation", args.checkpoint)
    )
    if args.once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=args.trigger)

    query = writer.start()
    query.awaitTermination()
    # Allow the async Py4J StreamingQueryListener thread to flush the final batch metrics
    time.sleep(0.3)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Prototype Job A: Kafka -> Spark Streaming -> Console")
    ap.add_argument(
        "--kafka",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (default: $KAFKA_BOOTSTRAP_SERVERS or localhost:9092)",
    )
    ap.add_argument(
        "--topic",
        default=StationStatusEnvelope.topic,
        help=f"Kafka topic to subscribe (default: {StationStatusEnvelope.topic})",
    )
    ap.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=["earliest", "latest"],
        help="Kafka startingOffsets when no checkpoint exists",
    )
    ap.add_argument(
        "--watermark",
        default="10 minutes",
        help="Spark watermark delay (e.g. '10 minutes')",
    )
    ap.add_argument(
        "--watermark-mode",
        default="last_reported",
        choices=list(VALID_WATERMARK_MODES),
        help="Column driving watermark ('last_reported' or 'snapshot_ts')",
    )
    ap.add_argument("--trigger", default="5 seconds", help="Micro-batch processingTime trigger")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=None, help="Optional maxOffsetsPerTrigger")
    ap.add_argument("--checkpoint", default="checkpoints/job_a_console", help="Spark checkpoint directory")
    ap.add_argument("--shuffle-partitions", type=int, default=4, help="spark.sql.shuffle.partitions")
    ap.add_argument("--num-rows", type=int, default=50, help="Max rows printed per micro-batch")
    ap.add_argument("--once", action="store_true", help="Process all available data and stop (availableNow=True)")
    return ap.parse_args()


if __name__ == "__main__":
    run(parse_args())
