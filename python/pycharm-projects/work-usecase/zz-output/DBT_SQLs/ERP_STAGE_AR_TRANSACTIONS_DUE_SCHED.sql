{%- set trimmed_source_name = model.name | replace("ERP_STAGE_","") -%}

{{
    config(
        alias= trimmed_source_name,
        materialized = 'table'
    )
}}

  SELECT 
        PAYMENT_SCHEDULE_ID, ACCOUNT_NUMBER, PARTY_NAME, CUSTOMER_TRX_ID, CLASS, TRANS_TYPE, TRANS_TYPE_NAME, CUST_TRX_TYPE_ID, INVOICE_CURRENCY_CODE,
        LOCATION_CODE, TRANSACTION_NUMBER, TRANS_DATE, GL_DATE_PS, DUE_DATE, AMOUNT_DUE_ORIGINAL, AMOUNT_DUE_ORIGINAL_EXCH, EXCHANGE_RATE,
        AMOUNT_DUE_REMAINING, ACCTD_AMOUNT_DUE_REMAINING, LAST_UPDATE_DATE, BU_NAME
        FROM
        {{ ref('ERP_STAGE_AR_TRANSACTIONS_DUE') }} 
        QUALIFY row_number() OVER (PARTITION BY PAYMENT_SCHEDULE_ID ORDER BY 1) = 1