from banxico import Banxico
import pandas as pd
from helper import log
from xrates import XRates

class XR_Banxico(XRates):
    def __init__(self, db):
        super().__init__(db)
        self.db = db
        self.exchange = Banxico()
        self.transactions_sql = """
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
