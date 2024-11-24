-- Databricks notebook source
-- MAGIC %md
-- MAGIC **SILVER LAYER**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC SCHEME TABLE

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_scheme
COMMENT "Silver Scheme table"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'SCHEME'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          ID: string,
          NAME: string,
          LOCATION: string,
          CREATED_BY: string,
          CREATED_ON: string,
          MULTI_SCHEME_ID: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string,
          TIMEZONE: string,
          PREFIX: string,
          COMMENCE_DATE: string,
          ENTITY_TYPE: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.ID AS ID,
  parsed_data.data.NAME AS NAME,
  parsed_data.data.LOCATION AS LOCATION,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON,
  parsed_data.data.TIMEZONE AS TIMEZONE,
  parsed_data.data.PREFIX AS PREFIX,
  TO_TIMESTAMP(parsed_data.data.COMMENCE_DATE) AS COMMENCE_DATE,
  parsed_data.data.ENTITY_TYPE AS ENTITY_TYPE
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_scheme
COMMENT "Merged Data Scheme Table"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_scheme
FROM
  STREAM(LIVE.silver_scheme)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC GEO_STATE TABLE

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_geo_state
COMMENT "GEO STATE TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'GEO_STATE'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          ID: string,
          NAME: string,
          CREATED_BY: string,
          CREATED_ON: string,
          MULTI_SCHEME_ID: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.ID AS ID,
  parsed_data.data.NAME AS NAME,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_geo_state
COMMENT "Geo State Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_geo_state
FROM
  STREAM(LIVE.silver_geo_state)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CRP_REFUND_TXN_DTL

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_crp_refund_txn_dtl
COMMENT "CRP_REFUND_TXN_DTL TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'CRP_REFUND_TXN_DTL'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          ID: string,
          TRANSACTION_ID: string,
          QUANTITY: string,
          REFUND_PER_UNIT: float,
          GROSS_AMOUNT: float,
          TAXABLE_AMOUNT: float,
          GST_AMOUNT: float,
          MATERIAL_TYPE_ID: string,
          CREATED_ON: string,
          MULTI_SCHEME_ID: string,
          LAST_MODIFIED_ON: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.ID AS ID,
  parsed_data.data.TRANSACTION_ID AS TRANSACTION_ID,
  parsed_data.data.QUANTITY AS QUANTITY,
  parsed_data.data.REFUND_PER_UNIT AS REFUND_PER_UNIT,
  parsed_data.data.GROSS_AMOUNT AS GROSS_AMOUNT,
  parsed_data.data.TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  parsed_data.data.GST_AMOUNT AS GST_AMOUNT,
  parsed_data.data.MATERIAL_TYPE_ID AS MATERIAL_TYPE_ID,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_crp_refund_txn_dtl
COMMENT "crp_refund_txn_dtl  Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_crp_refund_txn_dtl
FROM
  STREAM(LIVE.silver_crp_refund_txn_dtl)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CRP_REFUND_TXN_HDR

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
--VIC_NOP_A


CREATE OR REFRESH STREAMING LIVE TABLE silver_crp_refund_txn_hdr
COMMENT "CRP_REFUND_TXN_HDR TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'CRP_REFUND_TXN_HDR'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          ID: string,
          POS_DEVICE_ID: string,
          PAYMENT_METHOD: string,
          CUSTOMER_DECLARATION: string,
          TRANSACTED_ON: string,
          CREATED_ON: string,
          MULTI_SCHEME_ID: string,
          NOTIFIED: string,
          LAST_MODIFIED_ON: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.ID AS ID,
  parsed_data.data.POS_DEVICE_ID AS POS_DEVICE_ID,
  parsed_data.data.PAYMENT_METHOD AS PAYMENT_METHOD,
  parsed_data.data.CUSTOMER_DECLARATION AS CUSTOMER_DECLARATION,
  parsed_data.data.TRANSACTED_ON AS TRANSACTED_ON,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.NOTIFIED AS NOTIFIED,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;  

CREATE OR REFRESH STREAMING TABLE silver_merge_crp_refund_txn_hdr
COMMENT "crp_refund_txn_hdr Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_crp_refund_txn_hdr
FROM
  STREAM(LIVE.silver_crp_refund_txn_hdr)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CONSUMER_REFUND_TXN_DTL

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
-- CONSUMER_REFUND_TXN_DTL

CREATE OR REFRESH STREAMING LIVE TABLE silver_consumer_refund_txn_dtl
COMMENT "CONSUMER_REFUND_TXN_DTL TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'CONSUMER_REFUND_TXN_DTL'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          CREATED_BY: string,
          CREATED_ON: string,
          GROSS_AMOUNT: float,
          GST_AMOUNT: float,
          ID: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string,
          MATERIAL_TYPE_ID: string,
          MULTI_SCHEME_ID: string,
          QUANTITY: string,
          REFUND_PER_UNIT: float,
          TAXABLE_AMOUNT: float,
          TRANSACTION_ID: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.GROSS_AMOUNT AS GROSS_AMOUNT,
  parsed_data.data.GST_AMOUNT AS GST_AMOUNT,
  parsed_data.data.ID AS ID,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON,
  parsed_data.data.MATERIAL_TYPE_ID AS MATERIAL_TYPE_ID,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.QUANTITY AS QUANTITY,
  parsed_data.data.REFUND_PER_UNIT AS REFUND_PER_UNIT,
  parsed_data.data.TAXABLE_AMOUNT AS TAXABLE_AMOUNT,
  parsed_data.data.TRANSACTION_ID AS TRANSACTION_ID
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_merge_consumer_refund_txn_dtl
COMMENT "consumer_refund_txn_dtl Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_consumer_refund_txn_dtl
FROM
  STREAM(LIVE.silver_consumer_refund_txn_dtl)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CONSUMER_REFUND_TXN_HDR

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
-- CONSUMER_REFUND_TXN_HDR

CREATE OR REFRESH STREAMING LIVE TABLE silver_consumer_refund_txn_hdr
COMMENT "CONSUMER_REFUND_TXN_HDR TABLE"
TBLPROPERTIES ("quality" = "silver")
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'CES_MSCM'
    AND metadata_parsed.metadata.`table-name` = 'CONSUMER_REFUND_TXN_HDR'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          BAG_ISSUE_TYPE: string,
          CONSUMER_ID: string,
          CREATED_BY: string,
          CREATED_ON: string,
          CUSTOMER_DECLARATION: int,
          ID: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string,
          MSC_CONSUMER_SITE_ID: string,
          MULTI_SCHEME_ID: string,
          NOTIFIED: string,
          PAYMENT_METHOD: string,
          POS_DEVICE_ID: string,
          REFUND_CRP: string,
          STATUS: string,
          TRANSACTED_BY: string,
          TRANSACTED_ON: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.BAG_ISSUE_TYPE AS BAG_ISSUE_TYPE,
  parsed_data.data.CONSUMER_ID AS CONSUMER_ID,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.CUSTOMER_DECLARATION AS CUSTOMER_DECLARATION,
  parsed_data.data.ID AS ID,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON,
  parsed_data.data.MSC_CONSUMER_SITE_ID AS MSC_CONSUMER_SITE_ID,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.NOTIFIED AS NOTIFIED,
  parsed_data.data.PAYMENT_METHOD AS PAYMENT_METHOD,
  parsed_data.data.POS_DEVICE_ID AS POS_DEVICE_ID,
  parsed_data.data.REFUND_CRP AS REFUND_CRP,
  parsed_data.data.STATUS AS STATUS,
  parsed_data.data.TRANSACTED_BY AS TRANSACTED_BY,
  TO_TIMESTAMP(parsed_data.data.TRANSACTED_ON) AS TRANSACTED_ON
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_merge_consumer_refund_txn_hdr
COMMENT "consumer_refund_txn_hdr Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_consumer_refund_txn_hdr
FROM
  STREAM(LIVE.silver_consumer_refund_txn_hdr)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CONSUMER_TEST

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
-- CONSUMER_TEST

CREATE OR REFRESH STREAMING LIVE TABLE silver_consumer_test
COMMENT "CONSUMER_TEST TABLE"
TBLPROPERTIES ("quality" = "silver",'delta.feature.timestampNtz' = 'supported')
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
),
filtered_table AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    data_str,
    metadata_parsed.metadata.`schema-name` as METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation
  FROM metadata_parsed
  WHERE
    metadata_parsed.metadata.`schema-name` = 'KINESIS_GENERATOR'
    AND metadata_parsed.metadata.`table-name` = 'CONSUMER_TEST'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    METADATA_SCHEMA_NAME,
    METADATA_TABLE_NAME,
    operation,
    from_json(data_str, '
      struct<
        data: struct<
          AGE_BRACKET: string,
          AGE_CONFIRMATION: string,
          ID: string,
          CHARITY: string,
          CONTACT_FIRST_NAME: string,
          CONTACT_LAST_NAME: string,
          CREATED_BY: string,
          CREATED_ON: string,
          GENDER: string,
          GST_CONCESSION: string,
          GST_REGISTERED: string,
          INDIVIDUAL_CONSUMER: string,
          IS_COMMUNITY_GROUP: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string,
          MULTI_SCHEME_ID: string,
          SCHEME_PARTICIPANT_ID: string,
          SYNCED: string,
          TERMS_AND_CONDITIONS_ACCEPTED: string
        >
      >
    ') AS parsed_data
  FROM filtered_table
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.AGE_BRACKET AS AGE_BRACKET,
  parsed_data.data.AGE_CONFIRMATION AS AGE_CONFIRMATION,
  parsed_data.data.ID AS ID,
  parsed_data.data.CHARITY AS CHARITY,
  parsed_data.data.CONTACT_FIRST_NAME AS CONTACT_FIRST_NAME,
  parsed_data.data.CONTACT_LAST_NAME AS CONTACT_LAST_NAME,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.GENDER AS GENDER,
  parsed_data.data.GST_CONCESSION AS GST_CONCESSION,
  parsed_data.data.GST_REGISTERED AS GST_REGISTERED,
  parsed_data.data.INDIVIDUAL_CONSUMER AS INDIVIDUAL_CONSUMER,
  parsed_data.data.IS_COMMUNITY_GROUP AS IS_COMMUNITY_GROUP,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.SCHEME_PARTICIPANT_ID AS SCHEME_PARTICIPANT_ID,
  parsed_data.data.SYNCED AS SYNCED,
  parsed_data.data.TERMS_AND_CONDITIONS_ACCEPTED AS TERMS_AND_CONDITIONS_ACCEPTED,
  TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CURRENT_TIMESTAMP())) AS silver_layer_timestamp
FROM parsed_table
WHERE parsed_data IS NOT NULL
AND parsed_data.data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_merge_consumer_test
COMMENT "consumer_test Merge"
TBLPROPERTIES ("quality" = "silver");


APPLY CHANGES INTO
  live.silver_merge_consumer_test
FROM
  STREAM(LIVE.silver_consumer_test)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC TRANSACTION TEST

-- COMMAND ----------

-- Silver Layer: Parse and cleanse data from Bronze layer
-- TRANSACTION_TEST

CREATE OR REFRESH STREAMING LIVE TABLE silver_transaction_test
COMMENT "TRANSACTION_TEST TABLE"
TBLPROPERTIES ("quality" = "silver",'delta.feature.timestampNtz' = 'supported')
AS
WITH metadata_parsed AS (
  SELECT
    partitionKey,
    approximateArrivalTimestamp,
    data_str,
    sequenceNumber,
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ') AS metadata_parsed
  FROM STREAM(LIVE.bronze_kinesis_data)
  WHERE
    from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ').metadata.`schema-name` = 'KINESIS_GENERATOR'
    AND from_json(data_str, '
      struct<
        metadata: struct<
          `schema-name`: string,
          `table-name`: string,
          `operation`: string
        >
      >
    ').metadata.`table-name` = 'TRANSACTION_TEST'
),
parsed_table AS (
  SELECT 
    partitionKey,
    approximateArrivalTimestamp,
    sequenceNumber,
    metadata_parsed.metadata.`schema-name` AS METADATA_SCHEMA_NAME,
    metadata_parsed.metadata.`table-name` AS METADATA_TABLE_NAME,
    metadata_parsed.metadata.`operation` AS operation,
    from_json(data_str, '
      struct<
        data: struct<
          CONSUMER_ID: int,
          CREATED_BY: string,
          CREATED_ON: string,
          CUSTOMER_DECLARATION: int,
          AMOUNT: float,
          ID: string,
          LAST_MODIFIED_BY: string,
          LAST_MODIFIED_ON: string,
          MSC_CONSUMER_SITE_ID: string,
          MULTI_SCHEME_ID: string,
          NOTIFIED: int,
          PAYMENT_METHOD: string,
          POS_DEVICE_ID: string,
          STATUS: string,
          TRANSACTED_ON: string,
          YEAR: int,
          MONTH: int,
          DAY: int
        >
      >
    ') AS parsed_data
  FROM metadata_parsed
)
SELECT
  partitionKey,
  approximateArrivalTimestamp,
  sequenceNumber,
  METADATA_SCHEMA_NAME,
  METADATA_TABLE_NAME,
  operation,
  parsed_data.data.CONSUMER_ID AS CONSUMER_ID,
  parsed_data.data.CREATED_BY AS CREATED_BY,
  TO_TIMESTAMP(parsed_data.data.CREATED_ON) AS CREATED_ON,
  parsed_data.data.CUSTOMER_DECLARATION AS CUSTOMER_DECLARATION,
  parsed_data.data.AMOUNT AS AMOUNT,
  parsed_data.data.ID AS ID,
  parsed_data.data.LAST_MODIFIED_BY AS LAST_MODIFIED_BY,
  TO_TIMESTAMP(parsed_data.data.LAST_MODIFIED_ON) AS LAST_MODIFIED_ON,
  parsed_data.data.MSC_CONSUMER_SITE_ID AS MSC_CONSUMER_SITE_ID,
  parsed_data.data.MULTI_SCHEME_ID AS MULTI_SCHEME_ID,
  parsed_data.data.NOTIFIED AS NOTIFIED,
  parsed_data.data.PAYMENT_METHOD AS PAYMENT_METHOD,
  parsed_data.data.POS_DEVICE_ID AS POS_DEVICE_ID,
  parsed_data.data.STATUS AS STATUS,
  TO_TIMESTAMP(parsed_data.data.TRANSACTED_ON) AS TRANSACTED_ON,
  parsed_data.data.YEAR AS YEAR,
  parsed_data.data.MONTH AS MONTH,
  parsed_data.data.DAY AS DAY,
  TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', CURRENT_TIMESTAMP())) AS silver_layer_timestamp
FROM parsed_table
WHERE parsed_data IS NOT NULL AND parsed_data.data IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_merge_transaction_test
COMMENT "transaction_test Merge"
TBLPROPERTIES ("quality" = "silver");

APPLY CHANGES INTO
  live.silver_merge_transaction_test
FROM
  STREAM(LIVE.silver_transaction_test)
KEYS
  (ID)
APPLY AS DELETE WHEN
  operation = "delete"
SEQUENCE BY
  approximateArrivalTimestamp
COLUMNS * EXCEPT
  (operation, sequenceNumber)
STORED AS
  SCD TYPE 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC GOLD LAYER

-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE gold_scheme
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_scheme;


-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE gold_geo_state
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_geo_state;


-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE gold_crp_refund_txn_dtl
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_crp_refund_txn_dtl;

-- COMMAND ----------

SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_crp_refund_txn_hdr
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_crp_refund_txn_hdr;

-- COMMAND ----------

SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_consumer_refund_txn_hdr
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_consumer_refund_txn_hdr;

-- COMMAND ----------

SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_consumer_refund_txn_dtl
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT * FROM LIVE.silver_merge_consumer_refund_txn_dtl;

-- COMMAND ----------


SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_consumer_test
COMMENT "Data from gold layer"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
*
FROM LIVE.silver_merge_consumer_test;

-- COMMAND ----------

SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_transaction_test
COMMENT "Data from gold layer with random_date"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    *
FROM 
    LIVE.silver_merge_transaction_test;

-- COMMAND ----------

SET pipelines.trigger.interval=5 seconds;
CREATE OR REFRESH LIVE TABLE gold_consumer_transaction_test
COMMENT "Joined data from consumer and transaction with random_date"
TBLPROPERTIES ("quality" = "gold")
AS
SELECT 
    t.*,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', t.approximateArrivalTimestamp)) as approximateArrival_AU_TZ,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', t.CREATED_ON)) as CREATED_ON_AU_TZ,
    MAKE_DATE(t.YEAR, t.MONTH, t.DAY) AS random_date,
    DATEDIFF(
        SECOND, 
        TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', t.CREATED_ON)), 
        TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', t.approximateArrivalTimestamp))
    ) AS ingestion_seconds_to_bronze,
    DATEDIFF(
        SECOND, 
        TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', 'Australia/Sydney', t.CREATED_ON)), 
        t.silver_layer_timestamp
    ) AS ingestion_seconds_to_silver,
    c.AGE_BRACKET,
    c.AGE_CONFIRMATION,
    c.CHARITY,
    c.CONTACT_FIRST_NAME,
    c.CONTACT_LAST_NAME,
    c.CREATED_BY AS CONSUMER_CREATED_BY,
    c.CREATED_ON AS CONSUMER_CREATED_ON,
    c.GENDER,
    c.GST_CONCESSION,
    c.GST_REGISTERED,
    c.INDIVIDUAL_CONSUMER,
    c.IS_COMMUNITY_GROUP,
    c.LAST_MODIFIED_BY AS CONSUMER_LAST_MODIFIED_BY,
    c.LAST_MODIFIED_ON AS CONSUMER_LAST_MODIFIED_ON,
    c.MULTI_SCHEME_ID AS CONSUMER_MULTI_SCHEME_ID,
    c.SCHEME_PARTICIPANT_ID,
    c.SYNCED,
    c.TERMS_AND_CONDITIONS_ACCEPTED
FROM LIVE.silver_merge_transaction_test t
LEFT JOIN LIVE.silver_merge_consumer_test c
ON t.CONSUMER_ID = c.ID ;

