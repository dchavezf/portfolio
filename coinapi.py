from exchange import Exchange
import requests
from helper import set_candle
import os
from dotenv import load_dotenv

load_dotenv()

#==============================================================================================================
class CoinAPI(Exchange):
    def __init__(self, db):
        super().__init__(db)
        self.api_key = os.getenv("COINAPI_KEY")
        self.base_url = "https://rest.coinapi.io/v1"
        self.headers = {
            'Accept': 'text/plain',
            'Authorization': self.api_key
        }
        
        self.exchange="BITSO"
        # Orden de preferencia de exchanges
        self.exchanges = {
            "BITSO":"USD",
            "BINANCE": "USDT",
            "COINBASE": "USDT"
        }
        self.to_currency=self.exchanges[self.exchange]
        self.periods=["1SEC", "1MIN", "1HRS", "1DAY"]
        

    def get_ohlcv(self,from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type="SPOT"):
        pair = f"{exchange}_{pair_type}_{from_currency.upper()}_{to_currency.upper()}"
        url = f"{self.base_url}/ohlcv/{pair}/history"
        
        params = {
                "period_id": period_id,
                "time_start": start_time.isoformat().replace("+00:00", "Z"),
                "time_end": end_time.isoformat().replace("+00:00", "Z"),
                "limit": 200
        }
        
        response = requests.get(url, headers=self.headers, params=params, timeout=self.timeout)
        if response.status_code != 200:
            return None
        data = response.json()
        if not data:
            return None
        resultados = []
        for d in data:
            c = set_candle(
                        to_currency,
                        from_currency,
                        period_id,
                        d["time_period_start"],
                        d["time_period_end"],
                        d["time_open"],
                        d["time_close"],
                        d["price_open"],
                        d["price_close"],
                        exchange,
                        pair_type
                    )

            resultados.append(c)
        return resultados
        