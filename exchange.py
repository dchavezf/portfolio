import os
import pandas as pd
from exchangedb import ExchangeDb
from helper import calculate_periods, set_candle, log, datetime_utc
from datetime import datetime


#=====================================================================================    
class Exchange:
    def __init__(self, db):
        self.api_key = ""
        self.base_url=""
        self.headers = {}
        self.timeout = 30 # segundos
        self.exchange=""
        self.to_currency=""
        self.periods=[]
        self.exchangedb=ExchangeDb(db)
        self._debug=False

    def update_in_batch(self,sql):
        transactions_df = self.exchangedb.db.fetch_dataframe(sql)
        # Iterar sobre las transacciones faltantes (result_df)
        _debug=True 
        log("update",f"registros por procesar: {(len(transactions_df))}",_debug)
        for index, row in transactions_df.iterrows():            
            # 1. Obtener parámetros y precio externo
            from_currency = row['from_currency']
            dt = row['datetime']
            
            # Suponemos que el 'exchange' y otros campos se definen aquí o son constantes
            log("update",f"{from_currency} {dt}",_debug)
            price=self.get_value(from_currency, dt)
            if price:
                log("update",f"{from_currency} {dt} Price: {price['price']}",_debug)
            else:
                log("update",f"{from_currency} {dt} Price not found",_debug)
        log("update","Save")
        self.exchangedb.save()
        log("update","Listo")
    
    def get_value(self,from_currency:str, target_time:datetime):
        from_currency=from_currency.upper()
        target_time=datetime_utc(target_time)
        
        log("get_value",f"Intenta {from_currency}/{self.to_currency} {target_time}",self._debug)
        price=self.exchangedb.find_xrate(from_currency, self.to_currency, target_time)
        if price:
            return price
        
        for period_id in self.periods : 
            if price:
                return price
            candle = self.get_candle(from_currency, self.to_currency, target_time, period_id, self.exchange)
            if candle:
                price=self.exchangedb.add_xrate_row(                    
                    from_currency,
                    self.to_currency,
                    target_time,
                    candle
                )
                return price
        return None
    
    def get_candle(self, from_currency, to_currency, target_time, period_id, exchange, pair_type="SPOT"):     
        
        from_currency=from_currency.upper()
        to_currency=to_currency.upper()
        
        ohlcv=[]
        # hago loop para los 3 periodos: previous, current, next
        periods = calculate_periods(target_time, period_id)
        log("get_candle",periods,self._debug)
        for start_time, end_time in periods:
            log("get_candle",f"Intenta {from_currency}/{to_currency} {period_id} {start_time}",self._debug)
            period_ohlcv=self.exchangedb.find_history(from_currency, to_currency, period_id, start_time)
            
            if not period_ohlcv:
                try:
                    log("get_candle","Obtengo datos del Exchange",self._debug)
                    period_ohlcv = self.get_ohlcv(from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type)
                    if period_ohlcv:
                        self.exchangedb.add_history_rows(start_time, period_ohlcv)
                except Exception as e:
                    msg=f"Fallo al obtener datos del Exchange {e}"
                    log("get_candle", msg,True)
                    raise Exception(msg)
            if period_ohlcv:
                ohlcv.extend(period_ohlcv)
                
        # Busco la vela que este dentro del rango
        if not ohlcv:
            return None
        
        #convierte la columna 'time_period_start' en ohlvc usando la funcion datetime_utc
        for c in ohlcv:
            c["time_period_start"] = datetime_utc(c["time_period_start"])
            c["time_period_end"] = datetime_utc(c["time_period_end"])
            c["time_open"] = datetime_utc(c["time_open"])
            c["time_close"] = datetime_utc(c["time_close"])


        # ordena ohlvc por la propiedad time_period_start
        ohlcv.sort(key=lambda x: x['time_period_start'])
        log("get_candle",ohlcv,self._debug)
        max_candles=len(ohlcv)
        target_time_period = datetime_utc(target_time)
        log("get_candle",f"Busco vela para=> {target_time_period} dentro del rango",self._debug)
        for idx, candle in enumerate(ohlcv):
            candle_time_period_start = datetime_utc(candle['time_period_start'])
            candle_time_period_end = datetime_utc(candle['time_period_end'])

            log("get_candle", f"Veo si está entre {candle_time_period_start} y {candle_time_period_end}",self._debug)
            if target_time_period >= candle_time_period_start and target_time_period <= candle_time_period_end:
                return candle
            if idx<max_candles-1:
                next_candle=ohlcv[idx+1]
                next_candle_time_period_start =datetime_utc(next_candle['time_period_start'])

                # Esta entre velas
                log("get_candle", f"Veo entre  velas  {candle_time_period_end} y {next_candle_time_period_start}",self._debug)
                if  target_time_period >= candle_time_period_end and target_time_period < next_candle_time_period_start:
                    c = set_candle(
                        to_currency,
                        from_currency,
                        period_id,
                        candle["time_period_end"],
                        next_candle["time_period_start"],
                        candle["time_close"],
                        next_candle["time_open"],
                        candle["price_close"],
                        next_candle["price_open"],
                        candle["exchange"],
                        candle["pair_type"]
                    )
                    self.exchangedb.add_history_rows(start_time, [c])
                    return c          
        return None
    
    def get_values(self, from_currency, to_currency, empty_dates):
        pass
    
    def get_ohlcv(self, from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type):
        pass