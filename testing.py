from coinapi import CoinAPI
from exchangedb import ExchangeDb
from helper import str_to_datetime, log
from dbclass import DbClass


# -------------------------------------------------------------
#  Ejemplo de uso
# -------------------------------------------------------------

def test_coinapi():
    exchange = CoinAPI()
    ejemplos=[
        {"symbol":"btc","date":"2021-08-11T20:29:43.0000000Z"},
        {"symbol":"ada","date":"2022-05-30T11:20:31.0000000Z"}
    ]

    #coin_api._active_symbols('BITSO')
    
    for obj in ejemplos:
        symbol=obj["symbol"]
        ts=str_to_datetime(obj["date"])
        result = exchange.get_value(symbol, ts)
        if result:
            print(f"{symbol.upper()} {ts} Precio {result['price']:.15f}")
        else:
            print(f"No se encontró precio para {symbol.upper()} en {ts}")
    exchange.save()


    
def test_rebuild_exchanges():
    #db=DbClass('data/exchange/exchange.db')
    #db.execute_script('data/sql/export_candles.sql')
    # si existe el archivo data/exchange/XR_Candles.parquet compactalo con el nombre data/exchange/XR_Candles_{now.timestamp}.parquet
    #compress('data/exchange/XR_Candles.parquet')

    db=DbClass('data/portfolio/portfolio.db')
    db.execute_script('data/sql/schema_xr.sql')
    db.execute_script('data/sql/rebuild_exchange.sql')
    

def test_exchange_db():
    db=ExchangeDb()
    db._lastupdate()

def test_coinapi():
    log("main","Conectandose a base de datos")    
    db = DbClass("data/portfolio/portfolio.db")
    path="data/sql"
    
    log("main","Tipos de cambio USD")
    coinapi=CoinAPI(db)

    sql= """
        SELECT DISTINCT 
            t.datetime AS datetime,
            'USD' AS to_currency,
            currency AS from_currency,
            t.xr_usd
        FROM xr_transactions t 
        WHERE
            COALESCE(xr_usd,0) <=0
        ORDER BY from_currency, datetime
    """
    coinapi.update_in_batch(sql)
    log("main","Ajusto Tipos de Cambio USD")
    db.execute_script(f"{path}/transform_2.sql")
    
if __name__ == "__main__":        
    test_coinapi()
