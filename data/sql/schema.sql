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

create or REPLACE view xr_current_valuated_view as
    with xr as (
        select sum(case when currency='USD' then xr_mxn else 0 end) as xr_usd, 
            sum(case when currency='MXN' then 1/xr_udi else 0 end) as xr_udi
        from xr_current
    )
    select xr_usd, xr_udi, xr_udi/xr_usd as xr_udi_to_usd  from xr
;

CREATE OR REPLACE VIEW xr_transactions_valuated_view AS
    select         
        t.portfolio,
        ROW_NUMBER() OVER (
            PARTITION BY t.portfolio
            ORDER BY t.datetime, t.id
        ) AS portfolio_id,
        t.datetime,
        t.transaction_type,
        t.method,
        t.currency,
        t.from_xr_currency,
        t.net_amount,
        -- usd value at time of transaction
        t.usd_value,
        cast(
            cast(t.net_amount as double)*
            cast(p.xr_usd  as double) 
        as DECIMAL(20,8)) as current_usd,
        t.udi_value * x.xr_udi_to_usd as present_usd ,
        cast(
            cast(t.net_amount as double)*
            cast(p.xr_usd  as double) 
        as DECIMAL(20,8)) - (t.udi_value * x.xr_udi_to_usd) as pl_usd,

        -- mxn value at time of transaction
        t.mxn_value,
        cast(
            cast(t.net_amount as double)*
            cast(p.xr_mxn as double) 
        as DECIMAL(20,8)) as current_mxn,
        t.udi_value * x.xr_udi as present_mxn,
        cast(
            cast(t.net_amount as double)*
            cast(p.xr_mxn as double) 
        as DECIMAL(20,8)) - (t.udi_value * x.xr_udi) as pl_mxn ,

        -- udi value at time of transaction        
        t.udi_value,
        t.udi_value as present_udi,
        cast(
            cast(t.net_amount as double)*
            cast(p.xr_udi as double) 
        as DECIMAL(20,8)) as current_udi,

        cast(
            cast(t.net_amount as double)*
            cast(p.xr_udi as double) 
        as DECIMAL(20,8)) - t.udi_value as pl_udi   
    from xr_transactions t
    left join xr_current p ON
    t.from_xr_currency=p.currency 
    , xr_current_valuated_view x
;

CREATE OR REPLACE VIEW profit_analysis_view AS
    select 
        t.portfolio as portfolio,
        t.from_xr_currency as currency,
        sum(t.net_amount) as net_amount,

        sum(t.current_usd) as current_usd,
        sum(t.present_usd) as present_usd,
        sum(t.pl_usd) as pl_usd,

        sum(t.current_mxn) as current_mxn,
        sum(t.present_mxn) as present_mxn, 
        sum(t.pl_mxn) as pl_mxn,
 
        sum(T.current_udi) as current_udi,
        sum(t.present_udi) as present_udi,
        sum(t.pl_udi) as pl_udi
    from xr_transactions_valuated_view t
    group by 
        t.portfolio, 
        t.from_xr_currency
    order by 
        sum(t.pl_usd)  desc
