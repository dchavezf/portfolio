from dbclass import DbClass 
from xr_banxico import XR_Banxico
from xr_coinapi import XR_CoinAPI
from helper import log

log("Conectandose a base de datos")    
db = DbClass("data/exchange/exchange.db")
path="data/sql"
log("Creando tablas")
db.execute_script(f"{path}/schema_xr.sql")

log("Tipos de cambio MXN/UDI")
banxico=XR_Banxico(db)
banxico.transform()

log("Tipos de cambio USD")
coinapi=XR_CoinAPI(db)
coinapi.transform()

log("Listo")