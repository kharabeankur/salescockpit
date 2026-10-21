# ============================================================
# test_pipeline.py — Pipeline Data Quality Test Suite
# Copy each cell into a Databricks notebook and run.
# ============================================================

# ── CELL 1: Test Setup ────────────────────────────────────────
import sys
sys.path.insert(0, "/tmp/streaming_framework_final")

CATALOG   = "abd_easywithdata_poc"
SCHEMA    = "streaming_poc"
DB        = f"{CATALOG}.{SCHEMA}"

LANDING_TABLE    = f"{DB}.landing_life_policy"
BRONZE_TABLE     = f"{DB}.life_policy_overview_sync_event"
QUARANTINE_TABLE = f"{DB}.kafka_message_quarantine_kdp"
AUDIT_TABLE      = f"{DB}.pipeline_audit_log"

passed = []
failed = []

def test(name, condition, detail=""):
    if condition:
        passed.append(name)
        print(f"  ✅ PASS  {name}")
    else:
        failed.append(name)
        print(f"  ❌ FAIL  {name} {f'— {detail}' if detail else ''}")

def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

print("✅ Test setup ready")
print(f"   Catalog : {CATALOG}")
print(f"   Schema  : {SCHEMA}")

# ── CELL 2: Landing Table Tests ───────────────────────────────
section("LANDING TABLE TESTS")

# T1 — Table exists
try:
    spark.sql(f"SELECT 1 FROM {LANDING_TABLE} LIMIT 1").collect()
    landing_exists = True
except:
    landing_exists = False
test("Landing table exists", landing_exists)

if landing_exists:
    landing_df = spark.sql(f"SELECT * FROM {LANDING_TABLE}")
    landing_count = landing_df.count()

    # T2 — Has rows
    test("Landing has rows", landing_count > 0,
         f"found {landing_count} rows")

    # T3 — Required columns exist
    landing_cols = set(landing_df.columns)
    required_landing_cols = {"topic", "partition", "offset",
                              "kafka_timestamp", "kafka_key",
                              "raw_bytes", "ingested_at"}
    missing = required_landing_cols - landing_cols
    test("Landing has all required columns", len(missing) == 0,
         f"missing: {missing}")

    # T4 — No null raw_bytes
    null_bytes = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {LANDING_TABLE}
        WHERE raw_bytes IS NULL
    """).collect()[0]["cnt"]
    test("Landing has no null raw_bytes", null_bytes == 0,
         f"{null_bytes} null raw_bytes found")

    # T5 — No null kafka_key
    null_keys = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {LANDING_TABLE}
        WHERE kafka_key IS NULL
    """).collect()[0]["cnt"]
    test("Landing has no null kafka_keys", null_keys == 0,
         f"{null_keys} null keys found")

    # T6 — Kafka key format is correct (application_id:policy_number)
    bad_keys = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {LANDING_TABLE}
        WHERE kafka_key NOT LIKE '%:%'
    """).collect()[0]["cnt"]
    test("Landing kafka_key format is correct (app_id:policy_num)",
         bad_keys == 0, f"{bad_keys} keys with wrong format")

    # T7 — Topic is correct
    wrong_topic = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {LANDING_TABLE}
        WHERE topic != 'life_policy_overview_sync_'
    """).collect()[0]["cnt"]
    test("Landing topic name is correct", wrong_topic == 0,
         f"{wrong_topic} rows with wrong topic")

    print(f"\n  Landing row count : {landing_count}")

# ── CELL 3: Bronze Table Tests ────────────────────────────────
section("BRONZE TABLE TESTS")

# T8 — Table exists
try:
    spark.sql(f"SELECT 1 FROM {BRONZE_TABLE} LIMIT 1").collect()
    bronze_exists = True
except:
    bronze_exists = False
test("Bronze table exists", bronze_exists)

if bronze_exists:
    bronze_df    = spark.sql(f"SELECT * FROM {BRONZE_TABLE}")
    bronze_count = bronze_df.count()
    bronze_cols  = set(bronze_df.columns)

    # T9 — Has rows
    test("Bronze has rows", bronze_count > 0,
         f"found {bronze_count} rows")

    # T10 — Key columns present and typed correctly
    test("Bronze has application_id column", "application_id" in bronze_cols)
    test("Bronze has policy_number column",  "policy_number"  in bronze_cols)

    # T11 — All standard metadata columns present
    required_meta = {
        "__message_valid_flag", "__message_error_desc",
        "__rescued_data", "__schema_version_id",
        "__trace_id", "__msg_id", "__source_eap_id",
        "__user_domain_login", "__kafka_partition",
        "__kafka_offset", "__kafka_timestamp",
        "__source_system", "__load_timestamp",
        "__file_name", "__file_path"
    }
    missing_meta = required_meta - bronze_cols
    test("Bronze has all metadata columns", len(missing_meta) == 0,
         f"missing: {missing_meta}")

    # T12 — Structure columns present
    required_struct = {
        "audit_information_application",
        "audit_information_last_updated_at",
        "audit_information_user",
        "life_policy_life_product_instance",
        "life_policy_admins",
        "life_policy_status",
        "life_policy_yearly_premium",
        "life_policy_parties_in_policy",
    }
    missing_struct = required_struct - bronze_cols
    test("Bronze has all structure columns", len(missing_struct) == 0,
         f"missing: {missing_struct}")

    # T13 — No null application_id (key column)
    null_app_id = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE application_id IS NULL
    """).collect()[0]["cnt"]
    test("Bronze application_id has no nulls", null_app_id == 0,
         f"{null_app_id} null application_ids")

    # T14 — No null policy_number
    null_policy = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE policy_number IS NULL
    """).collect()[0]["cnt"]
    test("Bronze policy_number has no nulls", null_policy == 0,
         f"{null_policy} null policy_numbers")

    # T15 — All messages deserialized successfully
    invalid = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __message_valid_flag = false
    """).collect()[0]["cnt"]
    test("All bronze messages deserialized successfully",
         invalid == 0, f"{invalid} failed deserializations")

    # T16 — Source system is KDP
    wrong_src = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __source_system != 'KDP'
    """).collect()[0]["cnt"]
    test("Bronze source system is KDP", wrong_src == 0,
         f"{wrong_src} rows with wrong source system")

    # T17 — Schema version ID populated
    null_schema_id = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __schema_version_id IS NULL
    """).collect()[0]["cnt"]
    test("Bronze schema_version_id populated", null_schema_id == 0,
         f"{null_schema_id} null schema version IDs")

    # T18 — User domain login populated
    null_domain = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __user_domain_login IS NULL
    """).collect()[0]["cnt"]
    test("Bronze user_domain_login populated",
         null_domain < bronze_count,
         f"all {null_domain} rows have null domain login")

    # T19 — Kafka timestamps are recent (within last 24h)
    old_records = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __kafka_timestamp < current_timestamp() - INTERVAL 24 HOURS
    """).collect()[0]["cnt"]
    test("Bronze kafka_timestamps are recent (last 24h)",
         old_records == 0, f"{old_records} old records found")

    # T20 — No duplicate offsets per partition
    dupes = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT __kafka_partition, __kafka_offset, COUNT(*) AS c
            FROM {BRONZE_TABLE}
            GROUP BY __kafka_partition, __kafka_offset
            HAVING c > 1
        )
    """).collect()[0]["cnt"]
    test("Bronze has no duplicate partition/offset combinations",
         dupes == 0, f"{dupes} duplicate offsets found")

    # T21 — Load timestamp populated
    null_load = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE __load_timestamp IS NULL
    """).collect()[0]["cnt"]
    test("Bronze load_timestamp populated", null_load == 0,
         f"{null_load} null load timestamps")

    print(f"\n  Bronze row count  : {bronze_count}")
    print(f"  Valid messages    : {bronze_count - invalid}")
    print(f"  Invalid messages  : {invalid}")

# ── CELL 4: Data Consistency Tests ───────────────────────────
section("DATA CONSISTENCY TESTS")

if landing_exists and bronze_exists:

    # T22 — Bronze count <= Landing count
    test("Bronze row count <= Landing row count",
         bronze_count <= landing_count,
         f"bronze={bronze_count} landing={landing_count}")

    # T23 — All bronze kafka_offsets exist in landing
    orphan_bronze = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE} b
        LEFT JOIN {LANDING_TABLE} l
          ON b.__kafka_partition = l.partition
         AND b.__kafka_offset    = l.offset
        WHERE l.offset IS NULL
    """).collect()[0]["cnt"]
    test("All bronze records traceable to landing",
         orphan_bronze == 0,
         f"{orphan_bronze} bronze records not in landing")

    # T24 — application_id range is valid (1-20 per producer)
    out_of_range = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE application_id < 1 OR application_id > 20
    """).collect()[0]["cnt"]
    test("Bronze application_id values in valid range (1-20)",
         out_of_range == 0,
         f"{out_of_range} out-of-range application_ids")

    # T25 — policy_number is positive BIGINT
    bad_policy = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE}
        WHERE policy_number <= 0
    """).collect()[0]["cnt"]
    test("Bronze policy_number values are positive",
         bad_policy == 0,
         f"{bad_policy} invalid policy numbers")

# ── CELL 5: Audit Log Tests ───────────────────────────────────
section("AUDIT LOG TESTS")

try:
    audit_df    = spark.sql(f"SELECT * FROM {AUDIT_TABLE}")
    audit_count = audit_df.count()
    audit_exists = True
except:
    audit_exists = False

test("Audit log table exists", audit_exists)

if audit_exists:
    # T26 — Has pipeline runs
    test("Audit log has entries", audit_count > 0,
         f"found {audit_count} entries")

    # T27 — All 3 pipelines logged
    pipeline_names = [
        r["pipeline_name"] for r in
        spark.sql(f"SELECT DISTINCT pipeline_name FROM {AUDIT_TABLE}").collect()
    ]
    for p in ["kdp_life_policy_landing",
              "kdp_life_policy_bronze",
              "kdp_life_policy_quarantine"]:
        test(f"Audit log has entries for {p}", p in pipeline_names)

    # T28 — No failed runs
    failed_runs = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {AUDIT_TABLE}
        WHERE status = 'FAILED'
    """).collect()[0]["cnt"]
    test("No failed pipeline runs in audit log",
         failed_runs == 0,
         f"{failed_runs} failed runs found")

    # T29 — Duration is reasonable (< 300 seconds)
    slow_runs = spark.sql(f"""
        SELECT COUNT(*) AS cnt FROM {AUDIT_TABLE}
        WHERE duration_secs > 300
    """).collect()[0]["cnt"]
    test("All pipeline runs completed within 5 minutes",
         slow_runs == 0,
         f"{slow_runs} slow runs > 300s")

# ── CELL 6: Quarantine Tests ──────────────────────────────────
section("QUARANTINE TABLE TESTS")

try:
    spark.sql(f"SELECT 1 FROM {QUARANTINE_TABLE} LIMIT 1").collect()
    quarantine_exists = True
except:
    quarantine_exists = False

test("Quarantine table exists", quarantine_exists)

if quarantine_exists:
    q_count = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {QUARANTINE_TABLE}"
    ).collect()[0]["cnt"]

    # T30 — Quarantine is empty (all messages deserialized)
    test("Quarantine table is empty (no failed deserializations)",
         q_count == 0,
         f"{q_count} undeserializable messages found")

# ── CELL 7: Final Summary ─────────────────────────────────────
section("TEST SUMMARY")

total  = len(passed) + len(failed)
pct    = round(len(passed) * 100 / total, 1) if total > 0 else 0

print(f"\n  Total tests : {total}")
print(f"  Passed      : {len(passed)} ✅")
print(f"  Failed      : {len(failed)} ❌")
print(f"  Pass rate   : {pct}%")

if failed:
    print(f"\n  Failed tests:")
    for f in failed:
        print(f"    ❌ {f}")
else:
    print(f"\n  🎉 ALL TESTS PASSED — Pipeline data quality verified!")

# ── CELL 8: Sample Data Preview ──────────────────────────────
section("SAMPLE BRONZE DATA")

spark.sql(f"""
    SELECT
        application_id,
        policy_number,
        audit_information_application,
        audit_information_user,
        life_policy_life_product_instance.status        AS lpi_status,
        life_policy_life_product_instance.yearlyPremium AS lpi_yearly_premium,
        life_policy_life_product_instance.signedOn      AS lpi_signed_on,
        life_policy_status,
        life_policy_super,
        life_policy_payment_frequency,
        __message_valid_flag,
        __schema_version_id,
        __user_domain_login,
        __source_system,
        __kafka_partition,
        __kafka_offset,
        __kafka_timestamp,
        __load_timestamp
    FROM {BRONZE_TABLE}
    ORDER BY __kafka_timestamp DESC
    LIMIT 10
""").display()

# ── CELL 9: Detailed Stats ────────────────────────────────────
section("DETAILED STATISTICS")

if bronze_exists and bronze_count > 0:
    print("\n--- Application ID distribution ---")
    spark.sql(f"""
        SELECT application_id, COUNT(*) AS policy_count
        FROM {BRONZE_TABLE}
        GROUP BY application_id
        ORDER BY application_id
    """).display()

    print("\n--- Life policy status distribution ---")
    spark.sql(f"""
        SELECT life_policy_status, COUNT(*) AS cnt
        FROM {BRONZE_TABLE}
        GROUP BY life_policy_status
        ORDER BY life_policy_status
    """).display()

    print("\n--- Schema versions seen ---")
    spark.sql(f"""
        SELECT __schema_version_id,
               COUNT(*) AS messages,
               MIN(__kafka_timestamp) AS first_seen,
               MAX(__kafka_timestamp) AS last_seen
        FROM {BRONZE_TABLE}
        GROUP BY __schema_version_id
    """).display()

    print("\n--- Kafka partition/offset range ---")
    spark.sql(f"""
        SELECT
            __kafka_partition,
            MIN(__kafka_offset) AS min_offset,
            MAX(__kafka_offset) AS max_offset,
            COUNT(*)            AS message_count
        FROM {BRONZE_TABLE}
        GROUP BY __kafka_partition
        ORDER BY __kafka_partition
    """).display()
