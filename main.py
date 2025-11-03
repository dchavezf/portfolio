from dbclass import DbClass 
from banxico import Banxico
from coinapi import CoinAPI
from helper import log

try:
    log("main","Conectandose a base de datos")    
    db = DbClass("data/portfolio/portfolio.db")

    path="data/sql"
    log("main","Creando tablas")
    db.execute_script(f"{path}/schema.sql")
    
    log("main","Datacleansing")
    db.execute_script(f"{path}/datacleansing.sql")    

    log("main","Cargando tablas")
    db.execute_script(f"{path}/load.sql")

    log("main","Tipos de cambio MXN/UDI")
    banxico=Banxico(db)
    sql= """
        SELECT DISTINCT 
            CAST(DATE(t.datetime) AS TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'UDI' as from_currency
        FROM xr_transactions_view t
        LEFT JOIN XR_Prices b
            ON DATE(t.datetime) = b.target_time
            and b.to_currency='MXN'
            and b.from_currency='UDI'
        WHERE 
            b.price IS NULL
        UNION
        SELECT DISTINCT 
            CAST(DATE(t.datetime) AS TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'USD' as from_currency
        FROM xr_transactions_view t
        LEFT JOIN XR_Prices b
            ON DATE(t.datetime) = b.target_time
            and b.to_currency='MXN'
            and b.from_currency='USD'
        WHERE 
            b.price IS NULL
        UNION
        SELECT  
            cast(cURRENT_DATE as TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'USD' AS from_currency
        UNION
        SELECT  
            cast(cURRENT_DATE as TIMESTAMPTZ) AS datetime,
            'MXN' AS to_currency,
            'UDI' AS from_currency
        ;
    """
    banxico.update_in_batch(sql)
    log("main","Ajusto Tipos de Cambio MXN/UDI")
    db.execute_script(f"{path}/transform_1.sql")

    log("main","Tipos de cambio USD")
    coinapi=CoinAPI(db)

    sql= """
        SELECT DISTINCT 
            t.datetime AS datetime,
            'USD' AS to_currency,
            currency AS from_currency
        FROM xr_transactions t 
        WHERE
            COALESCE(xr_usd,0) <=0
        UNION
        SELECT DISTINCT 
            CAST(DATE(CURRENT_DATE) AS TIMESTAMPTZ) AS datetime,
            'USD' AS to_currency,
            from_xr_currency AS from_currency
        FROM xr_transactions t 
        ;
    """
    coinapi.update_in_batch(sql)
    log("main","Ajusto Tipos de Cambio USD")
    db.execute_script(f"{path}/transform_2.sql")
except Exception as e:
    log("main",f"Error: {e}",True)
log("main","Listo")
"""
TODO

- generacion de archivo de salida
- integracion de carga de archivos
"""