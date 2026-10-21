-- ============================================================
-- DDL: src_kdp bronze tables
-- Catalog : ${catalog}   (injected per bundle target)
-- Schema  : ${schema}    (injected per bundle target)
-- Source  : 11_Bronze_physical_model_databricks_v2.xlsx
-- ============================================================
-- Executed by the `setup` task of the bundle job on every run
-- (all statements are idempotent: CREATE ... IF NOT EXISTS).
-- Tables are pre-created with exact typed schema.
-- Pipelines write into these tables using mergeSchema=false.
-- ============================================================

USE CATALOG ${catalog};
USE SCHEMA ${schema};

-- ============================================================
-- 1. life_policy_overview_sync_event  (42 columns, src_kdp)
-- ============================================================
CREATE TABLE IF NOT EXISTS life_policy_overview_sync_event (

  -- key columns (from Kafka message key, split by ":")
  application_id                          INT               NOT NULL,
  policy_number                           BIGINT            NOT NULL,

  -- structure columns (parsed from Avro payload)
  audit_information_application           STRUCT<`id`: INT>,
  audit_information_last_updated_at       TIMESTAMP,
  audit_information_user                  STRUCT<`domain_login`: STRING>,

  life_policy_catalogue_life_brand        STRUCT<`id`: BIGINT>,

  life_policy_life_product_instance       STRUCT<
    `catalogue_product`: STRUCT<`code`: INT>,
    `signed_on`:         DATE,
    `status`:            INT,
    `valid_from`:        DATE,
    `valid_to`:          DATE,
    `yearly_premium`:    DECIMAL(38,2)
  >,

  life_policy_admins                      ARRAY<STRUCT<
    `domain_login`:      STRING,
    `registered_agent`:  STRUCT<`id`: INT, `order`: INT, `share`: DECIMAL(38,2)>,
    `role`:              INT
  >>,

  life_policy_application                 STRUCT<`id`: INT>,
  life_policy_cancelled_on                DATE,
  life_policy_company                     STRUCT<`id`: BIGINT>,
  life_policy_data_updated_at             TIMESTAMP,

  life_policy_legacy_generali             STRUCT<
    `cancellation_reason`: STRING,
    `quotation_number`:    STRING
  >,

  life_policy_number                      BIGINT,
  life_policy_original_valid_to           DATE,

  life_policy_parties_in_policy           ARRAY<STRUCT<
    `addresses`: STRUCT<
      `correspondence`: STRUCT<
        `additional_delivery_instruction`: STRING,
        `audit_information`: STRUCT<
          `application`:      STRUCT<`id`: INT>,
          `last_updated_at`:  TIMESTAMP,
          `user`:             STRUCT<`domain_login`: STRING>
        >,
        `country`:              STRING,
        `evidence_number`:      STRING,
        `ext_id`:               BIGINT,
        `land_register_number`: STRING,
        `municipality`:         STRING,
        `municipality_part`:    STRING,
        `ruian_address_point`:  STRUCT<`code`: BIGINT>,
        `street`:               STRING,
        `street_number`:        STRING,
        `zip_code`:             STRING
      >
    >,
    `electronic_contacts`: STRUCT<
      `email`: STRUCT<
        `audit_information`: STRUCT<
          `application`:      STRUCT<`id`: INT>,
          `last_updated_at`:  TIMESTAMP,
          `user`:             STRUCT<`domain_login`: STRING>
        >,
        `email_address`: STRING,
        `ext_id`:        BIGINT
      >,
      `fixed_line`: STRUCT<
        `audit_information`: STRUCT<
          `application`:      STRUCT<`id`: INT>,
          `last_updated_at`:  TIMESTAMP,
          `user`:             STRUCT<`domain_login`: STRING>
        >,
        `ext_id`: BIGINT,
        `phone_number_format_choice`: STRUCT<
          `international`: STRUCT<`full_number`: STRING>,
          `structured`:    STRUCT<`international_prefix`: INT, `national_number`: STRING>
        >
      >,
      `mobile_phone`: STRUCT<
        `audit_information`: STRUCT<
          `application`:      STRUCT<`id`: INT>,
          `last_updated_at`:  TIMESTAMP,
          `user`:             STRUCT<`domain_login`: STRING>
        >,
        `ext_id`: BIGINT,
        `phone_number_format_choice`: STRUCT<
          `international`: STRUCT<`full_number`: STRING>,
          `structured`:    STRUCT<`international_prefix`: INT, `national_number`: STRING>
        >
      >
    >,
    `party`: STRUCT<`party_id`: STRING>,
    `roles`: ARRAY<STRUCT<
      `party_system_record`: STRUCT<`ext_id`: BIGINT>,
      `type`:                INT
    >>
  >>,

  life_policy_payment_activation_deadline DATE,
  life_policy_payment_frequency           INT,
  life_policy_renewal_on                  DATE,
  life_policy_signed_on                   DATE,
  life_policy_status                      INT,
  life_policy_super                       BOOLEAN,

  life_policy_super_policy_reference      STRUCT<`number`: BIGINT>,

  life_policy_valid_from                  DATE,
  life_policy_valid_to                    DATE,
  life_policy_version_valid_from          DATE,
  life_policy_yearly_premium              DECIMAL(38,2),

  -- validation columns
  __message_valid_flag                    BOOLEAN           NOT NULL,
  __message_error_desc                    STRING,
  __rescued_data                          STRING,
  __schema_version_id                     INT,

  -- metadata columns
  __trace_id                              STRING,
  __msg_id                                STRING,
  __source_eap_id                         STRING,
  __user_domain_login                     STRING,
  __kafka_partition                        INT,
  __kafka_offset                          BIGINT,
  __kafka_timestamp                       TIMESTAMP,
  __source_system                         STRING,
  __load_timestamp                        TIMESTAMP,
  __file_name                             STRING,
  __file_path                             STRING

)
USING DELTA
CLUSTER BY (application_id, policy_number);

-- ============================================================
-- 2. life_policy_overview_delete_event  (22 columns, src_kdp)
-- ============================================================
CREATE TABLE IF NOT EXISTS life_policy_overview_delete_event (

  -- key columns
  application_id                          INT               NOT NULL,
  policy_number                           BIGINT            NOT NULL,

  -- structure columns
  audit_information_application           STRUCT<`id`: INT>,
  audit_information_last_updated_at       TIMESTAMP,
  audit_information_user                  STRUCT<`domain_login`: STRING>,
  policy_application                      STRUCT<`id`: INT>,
  policy_company                          STRUCT<`id`: BIGINT>,

  -- validation columns
  __message_valid_flag                    BOOLEAN           NOT NULL,
  __message_error_desc                    STRING,
  __rescued_data                          STRING,
  __schema_version_id                     INT,

  -- metadata columns
  __trace_id                              STRING,
  __msg_id                                STRING,
  __source_eap_id                         STRING,
  __user_domain_login                     STRING,
  __kafka_partition                        INT,
  __kafka_offset                          BIGINT,
  __kafka_timestamp                       TIMESTAMP,
  __source_system                         STRING,
  __load_timestamp                        TIMESTAMP,
  __file_name                             STRING,
  __file_path                             STRING

)
USING DELTA
CLUSTER BY (application_id, policy_number);

-- ============================================================
-- 3. kafka_message_quarantine  (21 columns, src_kdp)
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_message_quarantine_kdp (

  quarantine_id                           BIGINT            NOT NULL,
  kafka_topic                             STRING            NOT NULL,
  source_object                           STRING,
  raw_key                                 BINARY,
  raw_value                               BINARY,
  error_type_code                         STRING,
  error_desc                              STRING,
  schema_version_id                       INT,
  retry_count                             INT,
  resolved_flag                           BOOLEAN,

  -- metadata columns
  __trace_id                              STRING,
  __msg_id                                STRING,
  __source_eap_id                         STRING,
  __user_domain_login                     STRING,
  __kafka_partition                        INT,
  __kafka_offset                          BIGINT,
  __kafka_timestamp                       TIMESTAMP,
  __source_system                         STRING,
  __load_timestamp                        TIMESTAMP,
  __file_name                             STRING,
  __file_path                             STRING

)
USING DELTA;

-- ============================================================
-- 4. Landing table (not in bronze model, framework internal)
-- ============================================================
CREATE TABLE IF NOT EXISTS landing_life_policy (
  topic           STRING,
  partition       INT,
  offset          BIGINT,
  kafka_timestamp TIMESTAMP,
  kafka_key       STRING,
  raw_bytes       BINARY,
  ingested_at     TIMESTAMP
)
USING DELTA;

-- ============================================================
-- Verify
-- ============================================================
SHOW TABLES;
