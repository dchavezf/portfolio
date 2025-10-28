-- Ajuste a las transacciones de Conversion cuando esta invertido el par
update conversion 
set 
    price=1/price, 
    price_currency=from_currency 
where 
    price_currency= to_currency;

-- Uno todas las transacciones que fueron cargadas por separado
INSERT INTO xr_transactions
select 
    datetime, 
    timestamp, 
    transaction_type, 
    method, 
    id, 
    portfolio,  
    currency, 
    net_amount, 
    usd_value, 
    mxn_value, 
    udi_value, 
    xr_usd, 
    xr_mxn, 
    xr_udi, 
    from_xr_currency, 
    to_xr_currency, 
    price, 
    trn_update_id 
from xr_transactions_view;

-- Default conversion a USD
UPDATE xr_transactions
SET to_xr_currency='usd'
where to_xr_currency in ('usd', 'dai', 'usds', 'usdt');

UPDATE xr_transactions
SET from_xr_currency='usd'
where from_xr_currency in ('usd', 'dai', 'usds', 'usdt');

UPDATE xr_transactions 
set 
    price=1
where to_xr_currency=from_xr_currency;

-- PAR UDI/MXN
UPDATE xr_transactions t
SET 
    xr_mxn = b.price
FROM XR_Prices b
WHERE 
    CAST(DATE(t.datetime) AS TIMESTAMPTZ) = CAST(DATE(b.target_time) AS TIMESTAMPTZ)
    and b.to_currency='MXN'
    and b.from_currency='USD'
and b.price>0;
-- PAR UDI/MXN
UPDATE xr_transactions t
SET 
    xr_udi = b.price
FROM XR_Prices b
WHERE 
    CAST(DATE(t.datetime) AS TIMESTAMPTZ) = CAST(DATE(b.target_time) AS TIMESTAMPTZ)
    and b.to_currency='MXN'
    and b.from_currency='UDI'
and b.price>0;

-- Si el tipo de cambio fue definido en la transaccion y es USD, actualzo el tipo de cambio del pair
update  xr_transactions
set 
    xr_usd=price,
    trn_update_id=10
where 
    xr_usd is null and 
    to_xr_currency='usd' and
    COALESCE(price,0)>0;

-- Por default si el currecy base es USD entonces el tipo de cambio es 1
update  xr_transactions
set 
    xr_usd=1,
    trn_update_id=20
where 
    xr_usd is null and 
    from_xr_currency='usd' and 
    COALESCE(price,0)>0;

-- Si el from es MXN y el to es USD, actualizo el tipo de cambio de 1/xr_mxn a xr_usd
update  xr_transactions
set 
    xr_usd=1/xr_mxn,
    trn_update_id=30
where 
    xr_usd is null and 
    from_xr_currency='mxn' and 
    COALESCE(xr_mxn,0)>0
;

-- Este es el caso inverso
update  xr_transactions
set 
    xr_usd=price/xr_mxn,
    trn_update_id=40
where 
    xr_usd is null and 
    to_xr_currency='mxn' and 
    COALESCE(price,0)>0 AND
    COALESCE(xr_mxn,0)>0
    ;

