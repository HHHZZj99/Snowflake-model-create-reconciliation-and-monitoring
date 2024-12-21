use database KG;
use schema BI;


//Creating a Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_integration
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('hezijing99@qq.com');



SHOW NOTIFICATION INTEGRATIONS;


//Test email integration
CALL SYSTEM$SEND_EMAIL(
    'my_email_integration',    -- Recipient should be VARCHAR
    'hezijing99@qq.com',   
    'test email',
    'This is a test email to check email integration.' -- Body should be VARCHAR
);


//Create Alert Table only extract invalid rows
create or replace TABLE KG.BI.ALERT_TABLE (
	COMPANY_ACCOUNT VARCHAR(50),
	MERCHANT_ACCOUNT VARCHAR(100),
	PSP_REFERENCE VARCHAR(50),
	ORDER_NUMBER VARCHAR(200),
	PAYMENT_METHOD VARCHAR(30),
	BOOKING_DATE TIMESTAMP_TZ(9),
	TIMEZONE VARCHAR(30),
	DATE DATE,
	CURRENCY VARCHAR(30),
	AMOUNT NUMBER(38,0),
	RECORD_TYPE VARCHAR(30),
	PAYMENT_CURRENCY VARCHAR(20),
	RECEIVED NUMBER(38,0),
	AUTHORISED NUMBER(38,0),
	GROSS_DEBIT NUMBER(38,0),
	GROSS_CREDIT NUMBER(38,0),
	SETTLEMENT_CURRENCY VARCHAR(10),
	NET_CREDIT NUMBER(38,0),
	NET_DEBIT NUMBER(38,0),
	COMMISSION NUMBER(38,0),
	MARKUP NUMBER(38,0),
	SCHEME_FEES NUMBER(38,0),
	INTERCHANGE NUMBER(38,0),
	PROCESSING_FEE_CURRENCY VARCHAR(10),
	PROCESSING_FEE NUMBER(38,0),
	TRANSACTION_FEE NUMBER(38,0),
	USERNAME VARCHAR(100),
	PAYMENT_METHOD_VARIANT VARCHAR(100),
	ACQUIRER_AUTH_CODE VARCHAR(100),
	ACQUIRER_REFERENCE VARCHAR(100),
	MODIFICATION_MERCHANT_REFERENCE VARCHAR(100),
	MODIFICATION_PSP_REFERENCE VARCHAR(100),
	SALES_DATE DATE,
	NS_PUBLISHED_AT DATE,
	NS_ORDER_ID VARCHAR(200),
	NS_PAYMENT_METHOD VARCHAR(50),
	NS_PSP VARCHAR(100),
	NS_TRANSACTION_AMOUNT NUMBER(38,0),
	NS_TRANSACTION_CURRENCY VARCHAR(100),
	NS_MERCHANT_REFERENCE VARCHAR(200),
	NS_LOCATION NUMBER(38,0),
	NS_PAYLOAD_ID VARCHAR(200),
	NS_EXTERNAL_ID VARCHAR(100),
	IN_AD_ONLY VARCHAR(10),
	MERCHANT_PAYOUT NUMBER(38,0)
);


//Create table changes stream, only look at new rows instesd of all records, more efficiency
CREATE OR REPLACE STREAM table_changes_stream
ON TABLE prep_adyen_ns
SHOW_INITIAL_ROWS = FALSE;  


show streams like 'table_changes_stream';
//describe table prep_adyen_ns;



//Create procedure send email notification when model discrepency found
CREATE OR REPLACE PROCEDURE KG.BI.SEND_ADYEN_ALERTS()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var new_records_count = 0;

// Count new records in the stream
var sql_command = `SELECT COUNT(*) FROM table_changes_stream WHERE IN_AD_ONLY = 'Y'`;
var stmt = snowflake.createStatement({sqlText: sql_command});
var result_set = stmt.execute();

if (result_set.next()) {
    new_records_count = result_set.getColumnValue(1);
}

if (new_records_count > 0) {
    // Insert new records into the alert table
    var insert_command = `INSERT INTO alert_table (
        COMPANY_ACCOUNT,
        MERCHANT_ACCOUNT,
        PSP_REFERENCE,
        ORDER_NUMBER,
        PAYMENT_METHOD,
        BOOKING_DATE,
        TIMEZONE,
        DATE,
        CURRENCY,
        AMOUNT,
        RECORD_TYPE,
        PAYMENT_CURRENCY,
        RECEIVED,
        AUTHORISED,
        GROSS_DEBIT,
        GROSS_CREDIT,
        SETTLEMENT_CURRENCY,
        NET_CREDIT,
        NET_DEBIT,
        COMMISSION,
        MARKUP,
        SCHEME_FEES,
        INTERCHANGE,
        PROCESSING_FEE_CURRENCY,
        PROCESSING_FEE,
        TRANSACTION_FEE,
        USERNAME,
        PAYMENT_METHOD_VARIANT,
        ACQUIRER_AUTH_CODE,
        ACQUIRER_REFERENCE,
        MODIFICATION_MERCHANT_REFERENCE,
        MODIFICATION_PSP_REFERENCE,
        SALES_DATE,
        NS_PUBLISHED_AT,
        NS_ORDER_ID,
        NS_PAYMENT_METHOD,
        NS_PSP,
        NS_TRANSACTION_AMOUNT,
        NS_TRANSACTION_CURRENCY,
        NS_MERCHANT_REFERENCE,
        NS_LOCATION,
        NS_PAYLOAD_ID,
        NS_EXTERNAL_ID,
        IN_AD_ONLY,
        MERCHANT_PAYOUT
    )
    SELECT 
        COMPANY_ACCOUNT,
        MERCHANT_ACCOUNT,
        PSP_REFERENCE,
        ORDER_NUMBER,
        PAYMENT_METHOD,
        BOOKING_DATE,
        TIMEZONE,
        DATE,
        CURRENCY,
        AMOUNT,
        RECORD_TYPE,
        PAYMENT_CURRENCY,
        RECEIVED,
        AUTHORISED,
        GROSS_DEBIT,
        GROSS_CREDIT,
        SETTLEMENT_CURRENCY,
        NET_CREDIT,
        NET_DEBIT,
        COMMISSION,
        MARKUP,
        SCHEME_FEES,
        INTERCHANGE,
        PROCESSING_FEE_CURRENCY,
        PROCESSING_FEE,
        TRANSACTION_FEE,
        USERNAME,
        PAYMENT_METHOD_VARIANT,
        ACQUIRER_AUTH_CODE,
        ACQUIRER_REFERENCE,
        MODIFICATION_MERCHANT_REFERENCE,
        MODIFICATION_PSP_REFERENCE,
        SALES_DATE,
        NS_PUBLISHED_AT,
        NS_ORDER_ID,
        NS_PAYMENT_METHOD,
        NS_PSP,
        NS_TRANSACTION_AMOUNT,
        NS_TRANSACTION_CURRENCY,
        NS_MERCHANT_REFERENCE,
        NS_LOCATION,
        NS_PAYLOAD_ID,
        NS_EXTERNAL_ID,
        IN_AD_ONLY,
        MERCHANT_PAYOUT
    FROM table_changes_stream WHERE IN_AD_ONLY = 'Y'`; // Corrected here

    stmt = snowflake.createStatement({sqlText: insert_command});
    stmt.execute();

  

    // Send email notification when find new records
    var send_email_command = `
        CALL SYSTEM$SEND_EMAIL(
            'my_email_integration',
            'hezijing99@qq.com',
            'Adyen and Newstore transaction discrepency found',
            'A trasaction has been found in Adyen while missing in Newstore, please check!'
        )
    `;
    
    stmt = snowflake.createStatement({sqlText: send_email_command});
    stmt.execute();
}

return 'Alerts processed successfully';
$$;



//Create task to run above procedure every 5 minutes to monitor model transaction
CREATE OR REPLACE TASK adyen_transaction_alert
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON */5 * * * * Europe/London'  
AS
CALL send_adyen_alerts();


//Run task
ALTER TASK adyen_transaction_alert RESUME;







//Below are test prepared procedure. You can upload new raw data in internal stgae place and load it in prep_adyen_ns model
//Check whther new loaded records match both Adyen and NewStore system, if 'Y' found in flag, procedure and task should capture it and send rmail notification.
SELECT * FROM table_changes_stream WHERE IN_AD_ONLY = 'Y';



select * from prep_adyen_ns
where in_ad_only = 'Y';



COPY INTO prep_adyen_ns
FROM
(
    SELECT 
        $1 AS company_account,
        $2 AS merchant_account,
        $3 AS psp_reference,
        $4 AS order_number,
        $5 AS payment_method,
        CONVERT_TIMEZONE('UTC', $6):: TIMESTAMP_NTZ AS booking_date, 
        $7 AS timezone,
        TO_DATE($8, 'DD/MM/YYYY') AS date,
        $9 AS currency,
        $10 AS amount,
        $11 AS record_type,
        $12 AS payment_currency,
        $13 AS received,
        $14 AS authorised,
        $15 AS gross_debit,
        $16 AS gross_credit,
        $17 AS settlement_currency,
        $18 AS net_credit,
        $19 AS net_debit,
        $20::varchar AS commission,
        $21 AS markup,
        $22 AS scheme_fees,
        $23 AS interchange,
        $24 AS processing_fee_currency,
        $25 AS processing_fee,
        $26 AS transaction_fee,
        $27 AS user_name,
        $28 AS payment_method_variant,
        $29 AS acquirer_auth_code,
        $30::number AS acquirer_reference,
        $31::varchar AS modification_merchant_reference,
        $32 AS modification_psp_reference,
        $33::date AS sales_date, 
        TO_DATE($34, 'DD/MM/YYYY') AS ns_published_at,
        $35 AS ns_order_id,
        $36 AS ns_payment_method,
        $37 AS ns_psp,
        $38 AS ns_transaction_amount,
        $39 AS ns_transaction_currency,
        $40 AS ns_merchant_reference,
        $41 AS ns_location,
        $42 AS ns_payload_id,
        $43 AS ns_external_id,
        $44 AS in_ad_only,
        $45 AS merchant_payout
    FROM
    '@KG.BI.ADYEN/Adyen-NS (1).csv' 
    (FILE_FORMAT => 'adyen')
);
