# Databricks notebook source
# ============================================================
# Producer_notebook.py
# Standalone producer notebook (no framework import needed)
# Run BEFORE the framework to generate fresh data
# ============================================================

# ── CELL 1: Install ───────────────────────────────────────────
# COMMAND ----------

# MAGIC %pip install faker "confluent-kafka[avro,schemaregistry]>=2.6" "fastavro>=1.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# ── CELL 2: Config ───────────────────────────────────────────
import json, random, time, os
from datetime import date
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

fake = Faker()

dbutils.widgets.text("secret_scope", "streaming_framework")
SCOPE            = dbutils.widgets.get("secret_scope")

KAFKA_BOOTSTRAP  = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
KAFKA_API_KEY    = dbutils.secrets.get(SCOPE, "kafka-api-key")
KAFKA_API_SECRET = dbutils.secrets.get(SCOPE, "kafka-api-secret")
SR_URL           = "https://psrc-176j5l8.us-east1.gcp.confluent.cloud"
SR_API_KEY       = dbutils.secrets.get(SCOPE, "sr-api-key")
SR_API_SECRET    = dbutils.secrets.get(SCOPE, "sr-api-secret")
TOPIC            = "life_policy_overview_sync_"

print(f"KAFKA_API_KEY : {'✅' if KAFKA_API_KEY else '❌ MISSING'}")
print(f"SR_API_KEY    : {'✅' if SR_API_KEY    else '❌ MISSING'}")

# COMMAND ----------

# ── CELL 3: Helpers ───────────────────────────────────────────
def random_date_int(start_year=2010, end_year=2024):
    d = fake.date_between(start_date=date(start_year,1,1), end_date=date(end_year,12,31))
    return (d - date(1970,1,1)).days

def now_millis():
    return int(time.time() * 1000)

def decimal_to_bytes(value: float, scale: int = 2) -> bytes:
    int_val     = int(round(value * (10 ** scale)))
    byte_length = max(1, (int_val.bit_length() + 8) // 8)
    return int_val.to_bytes(byte_length, byteorder='big', signed=True)

def random_domain_login():
    return f"cpas\\{fake.user_name()[:8]}"

# COMMAND ----------

# ── CELL 4: Record generator ──────────────────────────────────
def generate_life_policy_record():
    audit_info = {
        "application": {"id": random.randint(1, 20)},
        "lastUpdatedAt": now_millis(),
        "user": {"domainLogin": random_domain_login()}
    }
    def make_admin():
        return {
            "domainLogin": random_domain_login(),
            "registeredAgent": {
                "id": random.randint(1000, 99999),
                "order": random.choice([None, 1, 2]),
                "share": None
            } if random.random() > 0.3 else None,
            "role": random.randint(1, 5)
        }
    def make_phone():
        return {
            "auditInformation": audit_info,
            "extId": random.randint(100000, 999999),
            "phoneNumberFormatChoice": {
                "international": {"fullNumber": f"+420{random.randint(600000000,799999999)}"},
                "structured": None
            }
        }
    def make_address():
        return {
            "additionalDeliveryInstruction": None,
            "auditInformation": audit_info,
            "country": random.choice(["CZE","SVK","AUT"]),
            "evidenceNumber": None,
            "extId": random.randint(100000, 999999),
            "landRegisterNumber": str(random.randint(1,500)),
            "municipality": fake.city()[:50],
            "municipalityPart": None,
            "ruianAddressPoint": {"code": random.randint(10000000,99999999)},
            "street": fake.street_name()[:50],
            "streetNumber": str(random.randint(1,200)),
            "zipCode": fake.postcode()[:10]
        }
    def make_party():
        return {
            "addresses": {"correspondence": make_address()},
            "electronicContacts": {
                "email": {
                    "auditInformation": audit_info,
                    "emailAddress": fake.email()[:100],
                    "extId": random.randint(100000, 999999)
                },
                "fixedLine": make_phone() if random.random() > 0.5 else None,
                "mobilePhone": make_phone() if random.random() > 0.3 else None
            },
            "party": {"partyId": str(fake.uuid4())[:36]},
            "roles": [
                {"partySystemRecord": {"extId": random.randint(100000,9999999)},
                 "type": random.randint(1,10)}
                for _ in range(random.randint(1,3))
            ]
        }

    signed_date = random_date_int(2005, 2022)
    valid_from  = signed_date
    valid_to    = valid_from + random.randint(365, 3650)
    premium     = round(random.uniform(500, 50000), 2)

    life_policy = {
        "catalogueLifeBrand": {"id": random.randint(1,50)} if random.random() > 0.2 else None,
        "lifeProductInstance": {
            "catalogueProduct": {"code": random.randint(100,999)},
            "signedOn": signed_date,
            "status": random.randint(1,5),
            "validFrom": valid_from,
            "validTo": valid_to if random.random() > 0.3 else None,
            "yearlyPremium": decimal_to_bytes(premium)
        },
        "admins": [make_admin() for _ in range(random.randint(1,3))],
        "application": {"id": random.randint(1,20)},
        "cancelledOn": random_date_int(2020,2024) if random.random() > 0.8 else None,
        "company": {"id": random.randint(1,10)},
        "dataUpdatedAt": now_millis(),
        "legacyGenerali": {
            "cancellationReason": fake.word()[:10] if random.random() > 0.7 else None,
            "quotationNumber": str(random.randint(1000000,9999999)) if random.random() > 0.7 else None
        } if random.random() > 0.5 else None,
        "number": random.randint(1000000000, 9999999999),
        "originalValidTo": random_date_int(2024,2030) if random.random() > 0.5 else None,
        "partiesInPolicy": [make_party() for _ in range(random.randint(1,4))],
        "paymentActivationDeadline": random_date_int(2024,2025) if random.random() > 0.7 else None,
        "paymentFrequency": random.randint(1,4),
        "renewalOn": random_date_int(2024,2026) if random.random() > 0.5 else None,
        "signedOn": signed_date if random.random() > 0.1 else None,
        "status": random.randint(1,5),
        "super": random.choice([True, False]),
        "superPolicyReference": {"number": random.randint(1000000000,9999999999)} if random.random() > 0.8 else None,
        "validFrom": valid_from if random.random() > 0.1 else None,
        "validTo": valid_to if random.random() > 0.3 else None,
        "versionValidFrom": random_date_int(2020,2024) if random.random() > 0.5 else None,
        "yearlyPremium": decimal_to_bytes(premium)
    }
    return {"auditInformation": audit_info, "lifePolicy": life_policy}

print("✅ Generator ready!")

# COMMAND ----------

# ── CELL 5: Produce ───────────────────────────────────────────
sr_client = SchemaRegistryClient({
    "url": SR_URL,
    "basic.auth.user.info": f"{SR_API_KEY}:{SR_API_SECRET}",
})
subject    = f"{TOPIC}-value"
latest     = sr_client.get_latest_version(subject)
schema_str = latest.schema.schema_str
print(f"✅ Schema fetched: {subject} (ID: {latest.schema_id})")

avro_serializer = AvroSerializer(sr_client, schema_str)
producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     KAFKA_API_KEY,
    "sasl.password":     KAFKA_API_SECRET,
})

NUM_RECORDS = 100
print(f"🚀 Producing {NUM_RECORDS} records...")

errors = []
for i in range(NUM_RECORDS):
    try:
        record     = generate_life_policy_record()
        # ── KEY FORMAT: application_id:policy_number ──────────
        # This matches the key_columns config in pipeline_config.py
        # Bronze will split this into two separate columns
        app_id     = record["auditInformation"]["application"]["id"]
        policy_num = record["lifePolicy"]["number"]
        kafka_key  = f"{app_id}:{policy_num}"

        producer.produce(
            topic = TOPIC,
            value = avro_serializer(
                record,
                SerializationContext(TOPIC, MessageField.VALUE)
            ),
            key   = kafka_key,  # ← "application_id:policy_number"
        )
        if (i+1) % 25 == 0:
            print(f"  {i+1}/{NUM_RECORDS} produced...")
    except Exception as e:
        errors.append(f"Record {i}: {e}")

producer.flush()
if errors:
    print(f"⚠ {len(errors)} errors: {errors[0]}")
else:
    print(f"✅ Done! {NUM_RECORDS} records → '{TOPIC}'")
    print(f"   Key format: application_id:policy_number")
    print(f"   Check: Confluent → Topics → {TOPIC} → Messages tab")
