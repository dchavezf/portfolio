
-- Actualizo los tipos de cambios directamente de los pares de la tabla de conversion
UPDATE xr_transactions
SET 
    xr_usd = c.price,
    trn_update_id=101
FROM XR_Prices AS c
WHERE 
    c.target_time = xr_transactions.datetime 
    AND  c.from_currency =xr_transactions.currency 
    AND c.to_currency = 'USD' 
    AND COALESCE(xr_transactions.xr_usd,0)<=0
    AND COALESCE(c.price, 0) > 0;
    
-- Actualiza montos
UPDATE xr_transactions
SET 
    -- 1. Valor en USD
    usd_value = (
        CAST(net_amount AS DOUBLE) * CAST(xr_usd AS DOUBLE)
    ),
    -- 2. Valor en MXN
    mxn_value = (
        CAST(net_amount AS DOUBLE) * CAST(xr_usd AS DOUBLE) * CAST(xr_mxn AS DOUBLE)
    ),
    
    -- 3. Valor en UDI
    udi_value = (
        (CAST(net_amount AS DOUBLE) * CAST(xr_usd AS DOUBLE) * CAST(xr_mxn AS DOUBLE)) / CAST(xr_udi AS DOUBLE)
    )
WHERE
    -- Aseguramos que los valores necesarios para el cálculo no sean nulos
    net_amount IS NOT NULL 
    AND xr_usd IS NOT NULL 
    AND xr_mxn IS NOT NULL
    AND xr_udi IS NOT NULL 
    AND xr_udi > 0; 

-- Actualiza portfolio
update xr_transactions
set portfolio=t.portfolio
from staging_transactions t 
WHERE
    --t.timestamp=xr_transactions.timestamp and 
    t.transaction_type=xr_transactions.transaction_type and 
    t.method=xr_transactions.method and 
    t."timestamp"=xr_transactions."timestamp" 
;
