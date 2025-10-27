import os
import pandas as pd
from exchangedb import ExchangeDb
from helper import calculate_periods, set_candle, log
from datetime import datetime
import json

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
        self.db=ExchangeDb(db)
        self._debug=False

    def get_value(self,from_currency:str, target_time:datetime):
        from_currency=from_currency.upper()
        if target_time.tzinfo is None:
            target_time=pd.to_datetime(target_time.tz_localize('UTC'))
        else:
            target_time=pd.to_datetime(target_time)
        log("get_value",f"Intenta {from_currency}/{self.to_currency} {target_time}",self._debug)
        price=self.db.find_xrate(from_currency, self.to_currency, target_time)
        if price:
            return price
        
        for period_id in self.periods : 
            if price:
                return price
            candle = self.get_candle(from_currency, self.to_currency, target_time, period_id, self.exchange)
            if candle:
                price=self.db.add_xrate_row(
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
            period_ohlcv=self.db.find_history(from_currency, to_currency, period_id, start_time)
            if not period_ohlcv:
                log("get_candle","Obtengo datos del Exchange",self._debug)
                period_ohlcv = self.get_ohlcv(from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type)
                if period_ohlcv:
                    self.db.add_history_rows(start_time, period_ohlcv)
            if period_ohlcv:
                ohlcv.extend(period_ohlcv)
                
        # Busco la vela que este dentro del rango
        if not ohlcv:
            return None
        # ordena ohlvc por la propiedad time_period_start
        ohlcv.sort(key=lambda x: x['time_period_start'])
        log("get_candle",ohlcv,self._debug)
        max_candles=len(ohlcv)
        target_time_period = pd.to_datetime(target_time)
        log("get_candle",f"Busco vela para=> {target_time_period} dentro del rango",self._debug)
        for idx, candle in enumerate(ohlcv):
            candle_time_period_start = pd.to_datetime(candle['time_period_start']).tz_convert('UTC')
            candle_time_period_end = pd.to_datetime(candle['time_period_end']).tz_convert('UTC')
            
            log("get_candle", f"Veo si está entre {candle_time_period_start} y {candle_time_period_end}",self._debug)
            if target_time_period >= candle_time_period_start and target_time_period <= candle_time_period_end:
                return candle
            if idx<max_candles-1:
                next_candle=ohlcv[idx+1]
                next_candle_time_period_start = pd.to_datetime(next_candle['time_period_start']).tz_convert('UTC')

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
                    self.db.add_history_rows(start_time, [c])
                    return c          
        return None
    
    def get_values(self, from_currency, to_currency, empty_dates):
        pass
    
    def get_ohlcv(self, from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type):
        pass