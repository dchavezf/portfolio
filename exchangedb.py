from dbclass import DbClass
import pandas as pd
from datetime import datetime

from helper import to_timestamp, cast_candle, compress, datetime_utc

#==============================================================================================================
class ExchangeDb:
    def __init__(self,db):
        self.path="data/exchange"
        self.db = db
        self.tables=["XR_Buckets", "XR_Candles", "XR_Prices", "XR_LastPeriods"]
        self._load_or_create_tables()

    def _load_or_create_tables(self):
        """
        Loads existing tables from the database or creates them if they don't exist.
        """
        for table_name in self.tables:
            try:
                # Try to load the table
                self.db.fetch_dataframe(f"SELECT * FROM {table_name} LIMIT 1")
            except Exception:
                # If loading fails, create the table
                self.db.execute_script(f"data/sql/schema_xr.sql")
                break

    def find_xrate(self, from_currency, to_currency, target_time):
        """
        Finds an exchange rate in the XR_Prices table.
        """
        sql = f"""
            SELECT *
            FROM XR_Prices
            WHERE
                to_currency = '{to_currency}' AND
                from_currency = '{from_currency}' AND
                target_time = '{target_time.isoformat().replace('+00:00', 'Z')}'
        """
        result = self.db.fetch_dataframe(sql)
        if not result.empty:
            return result.to_dict('records')[0]
        return None

    def find_history(self, from_currency, to_currency, period_id, dt_bucket):
        """
        Finds historical candle data in the XR_Candles table.
        """
        sql = f"""
            SELECT c.*
            FROM XR_Candles c
            JOIN XR_Buckets b ON c.xr_bucket_id = b.xr_bucket_id
            WHERE
                b.to_currency = '{to_currency}' AND
                b.from_currency = '{from_currency}' AND
                b.period_id = '{period_id}' AND
                b.dt_bucket = '{dt_bucket.isoformat().replace('+00:00', 'Z')}'
        """
        result = self.db.fetch_dataframe(sql)
        if not result.empty:
            return result.to_dict('records')
        return None

    def find_bucket(self, bucket_id ):
        sql=f"""
            SELECT *
            FROM XR_Buckets
            WHERE
                xr_bucket_id = '{bucket_id}'
        """
        result = self.db.fetch_dataframe(sql)
        if not result.empty:
            return result.to_dict('records')[0]
        return None

    def add_xrate_row(self,  from_currency, to_currency,target_time, candle):
        """
        Adds a new exchange rate to the XR_Prices table.
        """
        delta = min(
            abs(to_timestamp(candle["time_period_start"]) - to_timestamp(target_time)),
            abs(to_timestamp(candle["time_period_end"]) - to_timestamp(target_time))
        )

        price_obj = {
            "to_currency": to_currency,
            "from_currency": from_currency,
            "target_time": datetime_utc(target_time),
            "price": candle["price"],
            "delta": float(delta),
            "xr_candle_id": candle.get("xr_candle_id")
        }

        new_row_df = pd.DataFrame([price_obj])
        self.db.insert_dataframe(new_row_df, "XR_Prices")
        return price_obj

    def add_history_rows(self, dt_bucket_time, new_rows):
        """
        Adds new candle data to the XR_Candles table.
        """
        if not new_rows:
            return

        # First, get or create the bucket
        first_row = new_rows[0]
        to_currency = first_row["to_currency"]
        from_currency = first_row["from_currency"]
        period_id = first_row["period_id"]

        bucket_id = self._get_or_create_bucket(to_currency, from_currency, period_id, dt_bucket_time)

        processed_rows = []
        for row_data in new_rows:
            processed_candle = cast_candle(row_data)
            processed_candle["xr_bucket_id"] = bucket_id
            processed_rows.append(processed_candle)

        new_df = pd.DataFrame(processed_rows)
        
        # Drop columns that are not in XR_Candles
        columns_to_drop = ['to_currency', 'from_currency', 'period_id']
        new_df = new_df.drop(columns=[col for col in columns_to_drop if col in new_df.columns])

        self.db.insert_dataframe(new_df, "XR_Candles")
        candles=new_df.to_dict('records')
        #return {"bucket":bucket, "candles":candles}
        return candles

    def _lastupdate(self):
        """
        Genera una cadena de fecha que representa el primer día del mes actual 
        a medianoche (00:00:00.000) en formato ISO 8601 (YYYY-MM-DDT00:00:00.000Z).
        """
        now = datetime.now()
        today = datetime(now.year, now.month, now.day, 0, 0, 0, 0)
        first_day = datetime(now.year, now.month, 1, 0, 0, 0, 0)
        period_id=["1SEC", "1MIN", "1HRS", "1DAY"]
        lastdate=[today, today, today, first_day]
        
        # Construye un df con las columnas period_id y datetime
        df = pd.DataFrame({'period_id': period_id, 'datetime':lastdate})
        self.db.execute_sql("DELETE FROM XR_LastPeriods")
        self.db.insert_dataframe(df, "XR_LastPeriods")


    def _get_or_create_bucket(self, to_currency, from_currency, period_id, dt_bucket):
        """
        Gets the ID of an existing bucket or creates a new one and returns its ID.
        """
        sql = f"""
            SELECT xr_bucket_id
            FROM XR_Buckets
            WHERE
                to_currency = '{to_currency}' AND
                from_currency = '{from_currency}' AND
                period_id = '{period_id}' AND
                dt_bucket = '{dt_bucket.isoformat().replace('+00:00', 'Z')}'
        """
        result = self.db.fetch_dataframe(sql)
        if not result.empty:
            first_row = result.iloc[0].to_dict()['xr_bucket_id']
            return first_row

        if period_id == '1DAY':
            size_bucket = pd.Timestamp(dt_bucket).days_in_month
        elif period_id == '1HOUR':
            size_bucket = 24
        else:
            size_bucket = 60
        bucket_df = pd.DataFrame([{
            "to_currency": to_currency,
            "from_currency": from_currency,
            "period_id": period_id,
            "dt_bucket": datetime_utc(dt_bucket),
            "size_bucket": size_bucket
        }])
        self.db.insert_dataframe(bucket_df, "XR_Buckets")
        # Retrieve the id of the inserted row
        result = self.db.fetch_dataframe(sql)
        first_row = result.iloc[0].to_dict()['xr_bucket_id']

        return first_row
    
    def save(self):
        self._lastupdate()
        # Exporta XR_prices a archivo parquet
        sql="""
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
            ) TO 'data/exchange/XR_Candles.parquet' (FORMAT PARQUET);
        """
        self.db.execute_sql(sql)
        compress('data/exchange/XR_Candles.parquet')
