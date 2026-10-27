from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import *

# ==============================================================================
# BRONZE LAYER INGESTION - AZURE EVENT HUBS TO DELTA LIVE TABLES
# Streams real-time raw events from Azure Event Hubs via Kafka protocol
# ==============================================================================

# Event Hubs Configuration (Can also be retrieved dynamically from spark.conf)
EH_NAMESPACE = spark.conf.get("eh_namespace", "uberevents")
EH_NAME = spark.conf.get("eh_name", "ubertopic")
EH_CONN_STR = spark.conf.get("connection_string")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": 10000,
    "kafka.session.timeout.ms": 10000,
    "maxOffsetsPerTrigger": 10000,
    "failOnDataLoss": "false",
    "startingOffsets": "earliest"
}


@dp.table(
    comment="Raw streaming table ingesting rides from Azure Event Hubs topic"
)
def rides_raw():
    df = (
        spark.readStream.format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    # Cast binary Kafka value to string JSON
    df = df.withColumn("rides", col("value").cast("string"))
    return df
