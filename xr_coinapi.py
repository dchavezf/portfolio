from coinapi import CoinAPI
import pandas as pd
from helper import log

class XR_CoinAPI:
    def __init__(self, db):
        self.db = db
        self.exchange = CoinAPI()
        self.transactions_sql = """
            SELECT DISTINCT 
                t.datetime AS datetime,
                'USD' AS to_currency,
                upper(currency) AS from_currency
            FROM portfolio.public.transactions t
            LEFT JOIN XR_Prices b
                ON t.datetime = b.target_time and
                t.currency = b.from_currency AND
                b.to_currency='USD'
            WHERE 
                b.price IS NULL
            UNION
            SELECT DISTINCT 
                cast(cURRENT_DATE as timestamp) AS datetime,
                'USD' AS to_currency,
                upper(currency) AS from_currency
            FROM portfolio.public.transactions t
            ;
        """
        self.transactions_df=pd.DataFrame()

    def load(self):
        # The original query is trying to get transactions from a different database.
        # I will assume the transactions are in the same database as the exchange rates.
        # I will also assume the table is called 'transactions' in the 'public' schema.
        # This is a placeholder and might need to be adjusted based on the actual schema.
        try:
            self.transactions_df = self.db.fetch_dataframe(self.transactions_sql)
        except Exception as e:
            print(f"Error loading transactions: {e}")
            print("Using an empty DataFrame as a fallback.")
            self.transactions_df = pd.DataFrame(columns=['datetime', 'to_currency', 'from_currency'])

    def update(self):
        log(f"registros por procesar: {(len(self.transactions_df))}")
        for index, row in self.transactions_df.iterrows():
            from_currency = row['from_currency']
            dt = row['datetime']
            self.exchange.get_value(from_currency, dt)
    
    def save(self):
        # Recalculo xrates en base a lo que se extrajo
        self.exchange.save()
