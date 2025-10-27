COPY  (    
    select distinct  
        to_currency ,
        from_currency,
        period_id ,
        dt_bucket , 
        case 
            when period_id = '1DAY'  then day(last_day(dt_bucket))
            when period_id = '1HOUR' then 24 
            else 60 
        end size_bucket,
        time_period_start, time_period_end, time_open, time_close, price_open, price_high, price_low, price_close, volume_traded, trades_count,  COALESCE(delta,0) as delta, exchange, pair_type, price
    from "XR_Candles" c
    INNER join "XR_Buckets" b
    on  c.xr_bucket_id = b.xr_bucket_id
)
TO 'data/exchange/XR_Candles.parquet'
;
