delete from XR_Prices;
delete from XR_Candles;
delete from XR_buckets;

CREATE OR REPLACE TABLE history AS 
SELECT * FROM 'data/exchange/XR_Candles.parquet';

insert into "XR_Buckets" (to_currency, from_currency, period_id, dt_bucket, size_bucket)
select distinct
    to_currency ,
    from_currency,
    period_id ,
    dt_bucket , 
    size_bucket
from history;

insert into "XR_Candles" (xr_bucket_id, time_period_start, time_period_end, time_open, time_close, price_open, price_high, price_low, price_close, volume_traded, trades_count,  delta, exchange, pair_type, price)
select distinct xr_bucket_id, time_period_start, time_period_end, time_open, time_close, price_open, price_high, price_low, price_close, volume_traded, trades_count,  COALESCE(delta,0), exchange, pair_type, price
from history h
INNER join "XR_Buckets" b
on  h.to_currency = b.to_currency and
    h.from_currency = b.from_currency and
    h.period_id = b.period_id and
    h.dt_bucket=b.dt_bucket;
