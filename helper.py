from datetime import datetime, timedelta
import pandas as pd
from typing import List, Tuple, Dict, Any
import json
#=========================================================================
# Helpers
#=========================================================================
DEBUG=True
import zipfile
import os
import time
def compress(sourcefile):
    timestamp = int(time.time())
    source_file = f'{sourcefile}'
    destination_zip = f'{sourcefile}.{timestamp}.zip'

    with zipfile.ZipFile(destination_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(source_file, os.path.basename(source_file))
        
# Custom JSON encoder to handle datetime and Timestamp objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

def log(process, s, debug=DEBUG):
    if isinstance(s, (dict, list)):
        # Use the custom encoder with json.dumps
        s = json.dumps(s, indent=4, cls=DateTimeEncoder)
    
    if DEBUG:
        print(f"[{process}] {datetime.now()} -  {s}")
    
def to_timestamp(val):
        """Convierte str de fecha o float/int a timestamp float"""
        if val is None:
            return 0.0
        try:
            return float(val)
        except:
            try:
                return pd.to_datetime(val).timestamp()
            except:
                return 0.0

def floor_datetime(dt: datetime, floor_unit: str, delta: int) -> datetime:
    """
    Redondea un objeto datetime al inicio de la unidad especificada 
    y luego le suma un timedelta calculado a partir del entero 'delta' 
    y la 'floor_unit'.
    """
    
    # 1. Realizar el redondeo (Floor)
    if floor_unit == 'minute':
        floored_dt = dt.replace(second=0, microsecond=0)
        # Calcula el timedelta: el entero 'delta' representa minutos
        calculated_timedelta = timedelta(minutes=delta)
        
    elif floor_unit == 'hour':
        floored_dt = dt.replace(minute=0, second=0, microsecond=0)
        # Calcula el timedelta: el entero 'delta' representa horas
        calculated_timedelta = timedelta(hours=delta)

    elif floor_unit == 'day':
        floored_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # Calcula el timedelta: el entero 'delta' representa días
        calculated_timedelta = timedelta(days=delta)

    elif floor_unit == 'month':
        # Redondea al primer día del mes a las 00:00:00
        floored_dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        
        # NOTA: Python NO tiene un timedelta natural para meses. 
        # Lo más seguro es usar un promedio de días o la librería 'dateutil'.
        # Para mantener el código simple con 'datetime', se utiliza 'days' 
        # como base para el movimiento.
        # En este contexto, asumiré que un 'delta' de 1 para 'month' significa 1 mes
        # y usaré un enfoque de lógica de mes para mayor precisión.
        
        if delta != 0:
            # Lógica de meses: suma o resta meses enteros
            new_year = floored_dt.year
            new_month = floored_dt.month + delta
            
            # Ajustar el año si el mes se sale del rango (1-12)
            while new_month > 12:
                new_month -= 12
                new_year += 1
            while new_month <= 0:
                new_month += 12
                new_year -= 1
            
            # Reemplazar el mes y año, manteniendo el día 1 y la hora 00:00:00
            # Si se usa este método, NO se usa la suma de timedelta al final.
            return floored_dt.replace(year=new_year, month=new_month)
        
        # Si delta es 0, simplemente retorna el floor
        return floored_dt 

    else:
        raise ValueError(f"Invalid floor_unit: {floor_unit}")
        
    # 2. Sumar el timedelta (solo para minute, hour, day)
    return floored_dt + calculated_timedelta

def str_to_datetime(date_str: str) -> datetime:
    date_str=date_str.replace('Z', '+0000')
    ts=datetime.fromisoformat(date_str)
    return ts

def datetime_to_str(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")

def calculate_periods(target_time: datetime, period_id: str) -> List[Tuple[datetime, datetime]]:
    """
    Calcula un array con los start_time y end_time para el periodo anterior, 
    el periodo actual y el periodo siguiente.
    
    Returns:
        Una lista de tres tuplas [(start_anterior, end_anterior), (start_actual, end_actual), (start_siguiente, end_siguiente)].
    """
    
    # 1. Mapeo de Configuración: (unidad_de_redondeo, base_timedelta)
    config = {
        "1SEC": "minute",
        "1MIN": "hour",
        "1HRS": "day",
        "1DAY": "month", # Floor a Month, Base Delta 7 Days
    }

    if period_id not in config:
        raise ValueError(f"Invalid period_id: {period_id}")
    
    floor_unit = config[period_id]
    
    # 2. Calcular el tiempo redondeado (Floored Time)
    # Este 'floored_time' será el límite SUPERIOR del periodo ACTUAL.
    floored_time = []
    for i in range (4):
        floored_time.append (floor_datetime(target_time, floor_unit,i-1))
    
    periods = [
        (floored_time[0], floored_time[1]),
        (floored_time[1], floored_time[2]), 
        (floored_time[2], floored_time[3])
    ]
    
    return periods

def set_candle(
    to_currency,
    from_currency,
    period_id,
    time_period_start,
    time_period_end,
    time_open,
    time_close,
    price_open,
    price_close, 
    exchange,
    pair_type
):
    """Genera una nueva vela combinando propiedades de dos velas consecutivas."""
    c = {
        "to_currency": to_currency,
        "from_currency": from_currency,
        "period_id": period_id,
        "time_period_start": time_period_start,
        "time_period_end": time_period_end,
        "time_open": time_open,
        "time_close": time_close,
        "price_open": float(price_open),
        "price_high": max(float(price_close), float(price_open)),
        "price_low": min(float(price_close), float(price_open)),
        "price_close": float(price_close),
        "volume_traded": 0,
        "trades_count": 0,
        "delta":  0,
        "exchange": exchange,
        "pair_type": pair_type,
        "price": (float(price_open) + float(price_close)) / 2,
    }
    return cast_candle(c)

def cast_candle(candle):
    """
    Convierte una vela asegurando:
    - Todos los campos de tiempo (time_*) se devuelven como objetos datetime.
    - period_id, from_currency, to_currency, exchange, pair_type como str
    - price_*, price, volume_traded como float64
    - trades_count como Int64
    - delta como Int64
    """
    
    # 1. Propiedades de Identificación y Par (Strings)
    from_currency = str(candle.get("from_currency", ""))
    to_currency = str(candle.get("to_currency", ""))
    exchange = str(candle.get("exchange", ""))
    pair_type = str(candle.get("pair_type", ""))
    period_id = str(candle.get("period_id", ""))
    
    # 2. Tiempos (Objetos DATETIME)
    t_start = to_datetime(candle.get("time_period_start", 0))
    t_end   = to_datetime(candle.get("time_period_end", 0))
    t_open  = to_datetime(candle.get("time_open", 0))
    t_close = to_datetime(candle.get("time_close", 0))
    
    # 3. Precios (Float64)
    p_open  = float(pd.to_numeric(candle.get("price_open", 0), errors="coerce"))
    p_high = float(pd.to_numeric(candle.get("price_high", 0), errors="coerce"))
    p_low  = float(pd.to_numeric(candle.get("price_low", 0), errors="coerce"))
    p_close = float(pd.to_numeric(candle.get("price_close", 0), errors="coerce"))
    
    # Precio calculado
    price = pd.to_numeric(candle.get("price", (p_open + p_close) / 2), errors="coerce")
    
    # 4. Volúmenes y Delta
    volume_traded = pd.to_numeric(candle.get("volume_traded", 0), errors="coerce")
    trades_count  = pd.to_numeric(candle.get("trades_count", 0), errors="coerce")
    delta = int(to_timestamp(t_end) - to_timestamp(t_start))
    
    return {
        # Propiedades de Par/Identificación
        "to_currency": to_currency,
        "from_currency": from_currency,
        "period_id": period_id,
        
        # Tiempos (DATETIME)
        "time_period_start": t_start,
        "time_period_end": t_end,
        "time_open": t_open,
        "time_close": t_close,
        
        # Datos OHLCV
        "price_open": p_open,
        "price_high": p_high,
        "price_low": p_low,
        "price_close": p_close,
        "volume_traded": volume_traded,
        "trades_count": trades_count,
        
        # Datos calculados y parametros
        "delta": delta,
        "exchange": exchange,
        "pair_type": pair_type,
        "price": price,
    }

def to_datetime(value):
    """
    Convierte un valor de entrada (int, float, str, None) a un objeto datetime de Pandas.
    
    Args:
        value: El valor de tiempo a convertir (timestamp, cadena de fecha, o None).
    
    Returns:
        Un objeto pandas.Timestamp (datetime) o pandas.NaT si la conversión falla.
    """
    if value is None or (isinstance(value, (str)) and value.strip() == ""):
        # Maneja None o cadenas vacías/solo espacios
        return pd.NaT
    
    try:
        if isinstance(value, (int, float)):
            # Asume que si el número es grande (ej: > 10000), es un timestamp.
            # Convertimos el timestamp a segundos para compatibilidad con Pandas.
            # DuckDB a menudo usa microsegundos o nanosegundos, pero 's' es común.
            if value > 10**10: # Si es timestamp en milisegundos
                return pd.to_datetime(value, unit='ms', errors='coerce')
            elif value > 10**4: # Si es timestamp en segundos
                return pd.to_datetime(value, unit='s', errors='coerce')
            else:
                # Si es un número pequeño o cero, probablemente no es un timestamp válido
                return pd.NaT 
        
        # Si es una cadena, usa la conversión estándar de Pandas
        return pd.to_datetime(value, errors='coerce')
        
    except Exception:
        # En caso de cualquier otro error de conversión
        return pd.NaT