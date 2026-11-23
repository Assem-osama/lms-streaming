import csv
import json
import os
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError


# CONFIGURATION

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"
KAFKA_TOPIC = "raw_events"

CSV_FILE_PATH = "data/studentVle.csv"

MAX_RECORDS = 1000
REPLAY_DELAY_SECONDS = 0.3
#FLUSH_EVERY_RECORDS = 1000

EXPECTED_COLUMNS = [
    "code_module",
    "code_presentation",
    "id_student",
    "id_site",
    "date",
    "sum_click"
]


# KAFKA PRODUCER

def create_kafka_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

        key_serializer=lambda key: key.encode("utf-8"),

        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),

        acks="all", # wait ensure from kafka that the message is properly received by the broker and replicated to all in-sync replicas before considering it successfully sent.
        retries=10,
        max_in_flight_requests_per_connection=5, 

        # batch is 32 KB and wait 10ms for collect them then send them together to the broker.
        batch_size=32768,
        linger_ms=10,

        compression_type="gzip"
    )



# DATA VALIDATION

def validate_csv_columns(csv_reader):

    actual_columns = csv_reader.fieldnames

    if actual_columns != EXPECTED_COLUMNS:

        raise ValueError(
            f"Unexpected CSV columns: {actual_columns}"
        )


# DATA STREAMING

def stream_csv_to_kafka():

    producer = create_kafka_producer()

    total_records = 0
    start_time = time.time()

    try:

        with open(
            CSV_FILE_PATH,
            mode="r",
            encoding="utf-8",
            newline=""
        ) as csv_file:

            csv_reader = csv.DictReader(csv_file)

            validate_csv_columns(csv_reader)

            print("Starting CSV streaming...")
            print(f"Kafka Topic: {KAFKA_TOPIC}")
            print(f"Maximum Records: {MAX_RECORDS or 'Unlimited'}")

            for row in csv_reader:

                if MAX_RECORDS is not None and total_records >= MAX_RECORDS:
                    break

                student_id = row["id_student"]

                producer.send(
                    KAFKA_TOPIC,
                    key=student_id,
                    value=row
                )

                total_records += 1

                print(
                    f"[{total_records}] Sent -> Student: {student_id} | "
                    f"Course: {row['code_module']} | Site: {row['id_site']} | "
                    f"Clicks: {row['sum_click']}"
                )

                if REPLAY_DELAY_SECONDS > 0:
                    time.sleep(REPLAY_DELAY_SECONDS)

            producer.flush()


    except KeyboardInterrupt:

        print("\nStop requested by user.")

    except (OSError, ValueError, KafkaError) as error:

        print(f"Producer Error: {error}")
        raise

    finally:

        producer.flush()
        producer.close()

        elapsed_time = time.time() - start_time

        print("\nStreaming stopped.")
        print(f"Total Records Sent: {total_records:,}")
        print(f"Total Time: {elapsed_time:.2f} seconds")

# MAIN

if __name__ == "__main__":

    stream_csv_to_kafka()