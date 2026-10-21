"""
avro_producer.py — Generic Avro Data Producer
─────────────────────────────────────────────────────────────────
Produces fake Avro data to any Confluent Kafka topic.

Features:
- Fetches schema dynamically from Schema Registry (no hardcoding)
- Auto-generates realistic fake data from any Avro schema
- Smart field name detection (email, city, name, date, etc.)
- Handles complex types: nested records, arrays, unions, decimals

To add a new topic: add entry to TOPIC_DATA_GENERATORS below.

POC only — in production your real backend is the producer.
─────────────────────────────────────────────────────────────────
"""

import json
import os
import random
import time
from datetime import date, datetime
from faker import Faker

fake = Faker()


# ── Smart field name → realistic data generators ──────────────
# Checks if field name CONTAINS the pattern (case insensitive)
SMART_GENERATORS = {
    "email":        lambda: fake.email(),
    "firstname":    lambda: fake.first_name(),
    "lastname":     lambda: fake.last_name(),
    "name":         lambda: fake.name(),
    "city":         lambda: fake.city(),
    "country":      lambda: random.choice(["CZE", "SVK", "AUT", "DEU"]),
    "street":       lambda: fake.street_name()[:50],
    "zipcode":      lambda: fake.postcode()[:10],
    "zip":          lambda: fake.postcode()[:10],
    "phone":        lambda: f"+420{random.randint(600000000, 799999999)}",
    "mobile":       lambda: f"+421{random.randint(900000000, 999999999)}",
    "id":           lambda: random.randint(1000, 99999),
    "uuid":         lambda: str(fake.uuid4()),
    "domainlogin":  lambda: f"cpas\\{fake.user_name()[:8]}",
    "login":        lambda: fake.user_name()[:10],
    "url":          lambda: fake.url(),
    "ip":           lambda: fake.ipv4(),
    "timestamp":    lambda: int(time.time() * 1000),
    "updatedat":    lambda: int(time.time() * 1000),
    "createdat":    lambda: int(time.time() * 1000),
    "lastupdated":  lambda: int(time.time() * 1000),
    "signedon":     lambda: _random_date_int(2005, 2022),
    "validfrom":    lambda: _random_date_int(2010, 2022),
    "validto":      lambda: _random_date_int(2023, 2030),
    "cancelledon":  lambda: _random_date_int(2020, 2024),
    "date":         lambda: _random_date_int(2010, 2024),
    "amount":       lambda: round(random.uniform(100.0, 50000.0), 2),
    "premium":      lambda: _decimal_bytes(round(random.uniform(500, 50000), 2)),
    "salary":       lambda: round(random.uniform(30000, 200000), 2),
    "price":        lambda: round(random.uniform(1.0, 9999.9), 2),
    "quantity":     lambda: random.randint(1, 100),
    "number":       lambda: random.randint(1000000000, 9999999999),
    "policynum":    lambda: random.randint(1000000000, 9999999999),
    "status":       lambda: random.randint(1, 5),
    "frequency":    lambda: random.randint(1, 4),
    "share":        lambda: _decimal_bytes(round(random.uniform(0, 100), 2)),
    "order":        lambda: random.randint(1, 5),
    "role":         lambda: random.randint(1, 10),
    "code":         lambda: random.randint(100, 9999),
    "company":      lambda: random.randint(1, 10),
    "extid":        lambda: random.randint(100000, 9999999),
    "fullnumber":   lambda: f"+420{random.randint(600000000, 799999999)}",
    "municipality": lambda: fake.city()[:50],
    "super":        lambda: random.choice([True, False]),
}

# ── Type fallback generators ───────────────────────────────────
TYPE_GENERATORS = {
    "string":  lambda name: fake.word(),
    "long":    lambda name: int(time.time() * 1000),
    "int":     lambda name: random.randint(1, 99999),
    "double":  lambda name: round(random.uniform(0.1, 9999.9), 2),
    "float":   lambda name: round(random.uniform(0.1, 999.9), 2),
    "boolean": lambda name: random.choice([True, False]),
    "bytes":   lambda name: _decimal_bytes(round(random.uniform(1, 999), 2)),
}


# ── Helper functions ───────────────────────────────────────────
def _random_date_int(start_year=2010, end_year=2024) -> int:
    """Generate a random date as days since Unix epoch (Avro date type)."""
    d = fake.date_between(
        start_date=date(start_year, 1, 1),
        end_date=date(end_year, 12, 31)
    )
    return (d - date(1970, 1, 1)).days


def _decimal_bytes(value: float, scale: int = 2) -> bytes:
    """Convert a decimal value to Avro bytes representation."""
    int_val     = int(round(value * (10 ** scale)))
    byte_length = max(1, (int_val.bit_length() + 8) // 8)
    return int_val.to_bytes(byte_length, byteorder='big', signed=True)


# ── Core auto-generator ────────────────────────────────────────
def generate_value(field_name: str, avro_type, named_types: dict = None) -> any:
    """
    Generate a realistic fake value for a field.

    Priority order:
    1. Smart name match (field name contains pattern)
    2. Type-based fallback
    3. Generic string
    """
    if named_types is None:
        named_types = {}

    # handle union ["null", "string"] → pick non-null, randomly nullable
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if not non_null:
            return None
        # 20% chance of null for nullable fields
        if random.random() < 0.2:
            return None
        return generate_value(field_name, non_null[0], named_types)

    # string reference to named type
    if isinstance(avro_type, str):
        if avro_type in named_types:
            return generate_record(named_types[avro_type], named_types)
        gen = TYPE_GENERATORS.get(avro_type)
        if gen:
            return gen(field_name)
        return fake.word()

    if isinstance(avro_type, dict):
        rec_type = avro_type.get("type")

        # nested record → recurse
        if rec_type == "record":
            name = avro_type.get("name")
            if name:
                named_types[name] = avro_type
            return generate_record(avro_type, named_types)

        # array → generate 1-3 items
        if rec_type == "array":
            count = random.randint(1, 3)
            return [
                generate_value(field_name, avro_type["items"], named_types)
                for _ in range(count)
            ]

        # map → simple key-value
        if rec_type == "map":
            return {fake.word(): fake.word()}

        # bytes → decimal
        if rec_type == "bytes":
            return _decimal_bytes(round(random.uniform(100, 50000), 2))

        # wrapped scalar
        return generate_value(field_name, avro_type.get("type", "string"), named_types)

    # smart field name match
    field_clean = field_name.lower().replace("_", "").replace("-", "")
    for pattern, generator in SMART_GENERATORS.items():
        if pattern in field_clean:
            return generator()

    # type fallback
    gen = TYPE_GENERATORS.get(str(avro_type))
    if gen:
        return gen(field_name)

    return fake.word()


def generate_record(schema: dict, named_types: dict = None) -> dict:
    """Auto-generate a complete record matching any Avro schema."""
    if named_types is None:
        named_types = {}

    # cache named types for reference resolution
    name = schema.get("name")
    if name:
        named_types[name] = schema

    record = {}
    for field in schema.get("fields", []):
        record[field["name"]] = generate_value(
            field["name"], field["type"], named_types
        )
    return record


# ── Producer function ──────────────────────────────────────────
def produce_to_topic(
    topic:        str,
    num_records:  int  = 100,
    kafka_config: dict = None,
    sr_config:    dict = None,
):
    """
    Produce fake Avro records to a Confluent Kafka topic.

    Steps:
    1. Connect to Schema Registry
    2. Fetch schema dynamically (no hardcoding)
    3. Auto-generate realistic fake records
    4. Produce to topic

    Args:
        topic:        Confluent Kafka topic name
        num_records:  number of records to produce
        kafka_config: dict with bootstrap_servers, api_key, api_secret
        sr_config:    dict with url, api_key, api_secret
    """
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField

    # use env vars if config not passed
    if kafka_config is None:
        kafka_config = {
            "bootstrap_servers": os.environ.get("KAFKA_BOOTSTRAP", ""),
            "api_key":           os.environ.get("KAFKA_API_KEY", ""),
            "api_secret":        os.environ.get("KAFKA_API_SECRET", ""),
        }
    if sr_config is None:
        sr_config = {
            "url":      os.environ.get("SR_URL", ""),
            "api_key":  os.environ.get("SR_API_KEY", ""),
            "api_secret": os.environ.get("SR_API_SECRET", ""),
        }

    print(f"\n{'=' * 60}")
    print(f"AVRO PRODUCER")
    print(f"Topic   : {topic}")
    print(f"Records : {num_records}")
    print(f"{'=' * 60}")

    # ── Schema Registry connection ──
    sr_client = SchemaRegistryClient({
        "url": sr_config["url"],
        "basic.auth.user.info": f"{sr_config['api_key']}:{sr_config['api_secret']}",
    })

    subject    = f"{topic}-value"
    latest     = sr_client.get_latest_version(subject)
    schema_str = latest.schema.schema_str
    schema     = json.loads(schema_str)

    print(f"✅ Schema fetched: {subject} (ID: {latest.schema_id})")
    print(f"   Top-level fields: {[f['name'] for f in schema.get('fields', [])]}")

    avro_serializer = AvroSerializer(sr_client, schema_str)

    # ── Kafka Producer ──
    producer = Producer({
        "bootstrap.servers": kafka_config["bootstrap_servers"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     kafka_config["api_key"],
        "sasl.password":     kafka_config["api_secret"],
    })

    # ── Preview 2 sample records ──
    print(f"\n📋 Sample record preview:")
    for i in range(2):
        sample = generate_record(schema)
        # show only top-level field names and types
        preview = {k: type(v).__name__ for k, v in sample.items()}
        print(f"  Record {i+1}: {preview}")

    # ── Produce ──
    print(f"\n🚀 Producing {num_records} records...")
    errors = []

    for i in range(num_records):
        try:
            record = generate_record(schema)
            producer.produce(
                topic = topic,
                value = avro_serializer(
                    record,
                    SerializationContext(topic, MessageField.VALUE)
                ),
                key = str(i),
            )
            if (i + 1) % 25 == 0:
                print(f"  {i+1}/{num_records} produced...")
        except Exception as e:
            errors.append(f"Record {i}: {str(e)[:100]}")

    producer.flush()

    if errors:
        print(f"\n⚠ Completed with {len(errors)} errors:")
        for err in errors[:5]:
            print(f"  {err}")
    else:
        print(f"\n✅ Done! {num_records} Avro records produced to '{topic}'")
        print(f"   Check: Confluent → Topics → {topic} → Messages tab")

    print(f"{'=' * 60}\n")
    return len(errors) == 0
