CREATE OR REPLACE TABLE recent_candles AS
SELECT 
    c.xr_candle_id, 
    c.xr_bucket_id
FROM "XR_LastPeriods" AS i
INNER JOIN "XR_Buckets" b ON i.period_id = b.period_id
INNER JOIN "XR_Candles" c ON c.xr_bucket_id = b.xr_bucket_id
WHERE b.dt_bucket >= i.datetime;

DELETE from "XR_Prices"
where xr_candle_id in (select xr_candle_id from recent_candles);

DELETE from "XR_Candles"
where xr_bucket_id in (select xr_bucket_id from recent_candles);

DELETE from "XR_Buckets"
where xr_bucket_id in (select xr_bucket_id from recent_candles);

DELETE from "XR_Prices"
where target_time >= (SELECT datetime FROM "XR_LastPeriods" WHERE period_id='1DAY')
and xr_candle_id is Null   ;