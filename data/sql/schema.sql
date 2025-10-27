CREATE OR REPLACE TABLE conversion (
    from_currency      VARCHAR,
    to_currency        VARCHAR,
    from_amount        DECIMAL(18,8),
    to_amount          DECIMAL(18,8),
    price              DECIMAL(18,8),
    price_currency     VARCHAR,
    timestamp          BIGINT,
    datetime           VARCHAR
);

CREATE OR REPLACE TABLE earnings (
    method          VARCHAR,
    currency        VARCHAR,
    gross           DECIMAL(18,8),
    fee             DECIMAL(18,8),
    net_amount      DECIMAL(18,8),
    timestamp       BIGINT,
    datetime        VARCHAR
);

CREATE OR REPLACE TABLE funding (
    method          VARCHAR,
    currency        VARCHAR,
    gross           DECIMAL(18,8),
    fee             DECIMAL(18,8),
    net_amount      DECIMAL(18,8),
    timestamp       BIGINT,
    datetime        VARCHAR
);

CREATE OR REPLACE TABLE trade (
    type        VARCHAR,
    major       VARCHAR,
    minor       VARCHAR,
    amount      DECIMAL(18,8),
    rate        DECIMAL(18,8),
    value       DECIMAL(18,8),
    fee         DECIMAL(18,8),
    total       DECIMAL(18,8),
    timestamp   BIGINT,
    datetime    VARCHAR
);

 CREATE OR REPLACE TABLE withdrawal (
    method VARCHAR,
    currency VARCHAR,
    amount DOUBLE,
    timestamp BIGINT,
    datetime VARCHAR
);

CREATE OR REPLACE TABLE staging_transactions (
    datetime            TIMESTAMPTZ,
    timestamp           BIGINT,
    transaction_type    VARCHAR,
    method              VARCHAR,
    id                  INTEGER,
    portfolio           VARCHAR,
    currency            VARCHAR,
    net_amount          DECIMAL(38,18),
    usd_value           DECIMAL(20,4),
    mxn_value           DECIMAL(20,4),
    udi_value           DECIMAL(20,4),
    xr_usd              DECIMAL(20,8),
    xr_mxn              DECIMAL(20,8),
    xr_udi              DECIMAL(20,8)
);

CREATE OR REPLACE TABLE xr_transactions (
    datetime            TIMESTAMPTZ,
    timestamp           BIGINT,
    transaction_type    VARCHAR,
    method              VARCHAR,
    id                  INTEGER,
    portfolio           VARCHAR,
    currency            VARCHAR,
    net_amount          DECIMAL(38,18),
    usd_value           DECIMAL(20,4),
    mxn_value           DECIMAL(20,4),
    udi_value           DECIMAL(20,4),
    xr_usd              DECIMAL(20,8),
    xr_mxn              DECIMAL(20,8),
    xr_udi              DECIMAL(20,8),
    from_xr_currency    VARCHAR,
    to_xr_currency      VARCHAR,
    price               DECIMAL(20,8),
    trn_update_id       INTEGER 
);

CREATE OR REPLACE TABLE transactions (
    datetime            TIMESTAMPTZ,
    timestamp           BIGINT,
    transaction_type    VARCHAR,
    method              VARCHAR,
    id                  INTEGER,
    portfolio           VARCHAR,
    currency            VARCHAR,
    net_amount          DECIMAL(38,8),
    usd_value           DECIMAL(20,4),
    mxn_value           DECIMAL(20,4),
    udi_value           DECIMAL(20,4)
);

CREATE OR REPLACE TABLE XRates (
    to_currency         VARCHAR,
    from_currency       VARCHAR,
    datetime            TIMESTAMPTZ,
    timestamp           BIGINT,
    period_id           VARCHAR,
    target_time         VARCHAR, -- Mantener como VARCHAR ya que se almacena como string
    delta               INTEGER,
    exchange            VARCHAR,
    price               DECIMAL(20,8)
);

CREATE OR REPLACE VIEW xr_transactions_view AS
    SELECT  
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        ROW_NUMBER() OVER (ORDER BY timestamp) AS id,
        NULL AS portfolio,
        'funding' AS transaction_type,
        method,
        currency,
        net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        currency as from_xr_currency,
        'usd' as to_xr_currency,
        NULL as price,
        NULL as trn_update_id
    FROM funding
    -- earnings_view 
    UNION
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        ROW_NUMBER() OVER (ORDER BY timestamp) AS id,
        NULL AS portfolio,
        'earnings' AS transaction_type,
        method,
        currency,
        net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        currency as from_xr_currency,
        'usd' as to_xr_currency,
        NULL as price,
        NULL as trn_update_id
    FROM earnings
    UNION
    -- CONVERSION FROM
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2-1 AS id,
        NULL AS portfolio,
        'conversion' AS transaction_type,
        'from' AS   method,
        from_currency as currency,
        -from_amount AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        from_currency as from_xr_currency,
        to_currency as to_xr_currency,
        1/price as price,
        NULL as trn_update_id
    FROM conversion
    union
    -- CONVERSION TO
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2 AS id,
        NULL AS portfolio,
        'conversion' AS transaction_type,
        'to' AS   method,
        to_currency as currency,
        to_amount AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        to_currency as from_xr_currency,
        from_currency as to_xr_currency,
        price,
        NULL as trn_update_id
    FROM conversion
    UNION
    -- TRADE
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2-1 AS id,
        NULL AS portfolio,
        'trade' AS transaction_type,
        type AS method,
        major as currency,
        -amount AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        major as from_xr_currency,
        minor as to_xr_currency,
        rate as price,
        NULL as trn_update_id
    FROM trade
    WHERE type='sell'
    UNION
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2  AS id,
        NULL AS portfolio,
        'trade' AS transaction_type,
        'buy' AS method,
        minor as currency,
        total AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        minor as from_xr_currency,
        major as to_xr_currency,
        1/rate as price,
        NULL as trn_update_id
    FROM trade
    WHERE type='sell'
    UNION
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2-1 AS id,
        NULL AS portfolio,
        'trade' AS transaction_type,
        'sell' AS method,
        minor as currency,
        -value AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        minor as from_xr_currency,
        major as to_xr_currency,
        1/rate as price,
        NULL as trn_update_id
    FROM trade
    WHERE type='buy'
    UNION
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        (ROW_NUMBER() OVER (ORDER BY timestamp))*2 AS id,
        NULL AS portfolio,
        'trade' AS transaction_type,
        'buy' AS method,
        major as currency,
        amount AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        major as from_xr_currency,
        minor as to_xr_currency,
        rate as price,
        NULL as trn_update_id
    FROM trade
    WHERE type='buy'
    -- WITHDRAWAL
    UNION
    SELECT
        TO_TIMESTAMP(timestamp) AS datetime,
        timestamp,
        ROW_NUMBER() OVER (ORDER BY timestamp) AS id,
        NULL AS portfolio,
        'withdrawal' AS transaction_type,
        method,
        currency,
        -amount AS net_amount,
        NULL AS usd_value,
        NULL AS mxn_value,
        NULL AS udi_value,
        NULL AS xr_usd,
        NULL AS xr_mxn,
        NULL AS xr_udi,
        currency as from_xr_currency,
        'usd' as to_xr_currency,
        NULL as price,
        NULL as trn_update_id
    FROM withdrawal;
-- TERMINA CREACION DE VIEW xr_transactions_view