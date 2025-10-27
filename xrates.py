import pandas as pd
from helper import log

class XRates:
    def __init__(self, db):
        self.db = db
        self.exchange =None
        self.transactions_sql = ""
        self.transactions_df=pd.DataFrame()
        self.date_only = False
        self._debug=True
    
    def transform(self):
        self.load()
        self.update()
        self.save()

    def load(self):
        self.transactions_df = self.db.fetch_dataframe(self.transactions_sql)
    
    def save(self):
        # Recalculo xrates en base a lo que se extrajo
        self.exchange.db.save()

    def update(self):
        # Iterar sobre las transacciones faltantes (result_df)
        log("update",f"registros por procesar: {(len(self.transactions_df))}",self)
        for index, row in self.transactions_df.iterrows():            
            # 1. Obtener parámetros y precio externo
            from_currency = row['from_currency']
            dt = row['datetime']
            
            # Suponemos que el 'exchange' y otros campos se definen aquí o son constantes
            log("update",f"{from_currency} {dt}",self._debug)
            self.exchange.get_value(from_currency, dt)
