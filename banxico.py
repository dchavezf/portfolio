from datetime import datetime, timedelta
from exchange import Exchange
from helper import datetime_to_str, set_candle
import requests
import os
from dotenv import load_dotenv

load_dotenv()
#==============================================================================================================


class Banxico(Exchange):
    def __init__(self, db):
        super().__init__(db)
        self.api_key = os.getenv("BANXICO_API_KEY")
        self.base_url = "https://www.banxico.org.mx/SieAPIRest/service/v1"
        self.headers ={
            'Bmx-Token': self.api_key,
            'Accept': 'application/json'
        }
        
        self.exchange="BANXICO"
        # Orden de preferencia de exchanges
        self.series={
            "UDI" : "SP68257",
            "USD" : "SF43718"
        }
        self.to_currency="MXN"
        self.periods=[ "1DAY"]

    def get_ohlcv(self,from_currency, to_currency, start_time, end_time, period_id, exchange, pair_type="SPOT"):  
        from_currency=from_currency.upper()        
        serie=self.series.get(from_currency)
            
        if not serie:
            print(f"Serie no reconocida para la moneda: {from_currency}")
            return None 
        
        # Formatear fecha para Banxico (YYYY-MM-DD)
        fecha_inicio_str = start_time.strftime('%Y-%m-%d')
        if end_time:
            fecha_fin_str = end_time.strftime('%Y-%m-%d')
        else:
            fecha_fin_str = fecha_inicio_str
        
        url = f"https://www.banxico.org.mx/SieAPIRest/service/v1/series/{serie}/datos/{fecha_inicio_str}/{fecha_fin_str}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=self.timeout)
            response.raise_for_status()
            data= response.json()
        except requests.exceptions.HTTPError as e:
            # Menos verboso para errores HTTP
            status_code = e.response.status_code
            error_msg = f"Error HTTP {status_code}. Detalles en la respuesta de Banxico."
            try:
                # Intenta obtener un mensaje de error conciso del cuerpo de la respuesta
                data = e.response.json()
                if 'error' in data:
                    error_msg += f" Mensaje: {data['error']}"
            except:
                pass # Ignora si no puede parsear el JSON de error
            raise Exception(f"Error al consultar Banxico ({url}): {error_msg}")        
        except requests.exceptions.RequestException as e:
            raise Exception(f"Error de conexión con Banxico: {e}")
        except Exception as e:
            raise Exception(f"Error inesperado durante la petición: {e}")
 
        resultados = []
        if (data['bmx']['series'] and 
            len(data['bmx']['series']) > 0 and
            'datos' in data['bmx']['series'][0] and
            len(data['bmx']['series'][0]['datos']) > 0):
            for dato in data['bmx']['series'][0]['datos']:
                    fecha = dato['fecha']
                    fecha_open = datetime.strptime(fecha, '%d/%m/%Y')                    
                    fecha_close = datetime_to_str(fecha_open+timedelta(days=1))

                    valor = float(dato['dato'])
                    c = set_candle(
                        to_currency,
                        from_currency,
                        period_id,
                        fecha_open,
                        fecha_close,
                        fecha_open,
                        fecha_close,
                        valor,
                        valor,
                        exchange,
                        pair_type
                    )
                    resultados.append(c)
            return resultados     
        return None
