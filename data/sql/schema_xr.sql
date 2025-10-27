-- Crear secuencias para autoincrementar IDs
CREATE SEQUENCE IF NOT EXISTS xr_bucket_seq START 1;
CREATE SEQUENCE IF NOT EXISTS xr_candle_seq START 1;

-- Tabla principal de buckets
CREATE TABLE IF NOT EXISTS XR_Buckets (
    xr_bucket_id INTEGER PRIMARY KEY DEFAULT nextval('xr_bucket_seq'),
    to_currency VARCHAR(10) NOT NULL,
    from_currency VARCHAR(10) NOT NULL,
    period_id VARCHAR(20) NOT NULL,
    dt_bucket TIMESTAMPTZ NOT NULL,
    size_bucket INTEGER NOT NULL,
    UNIQUE (to_currency, from_currency, period_id, dt_bucket)
);

-- Tabla de velas (candles)
CREATE TABLE IF NOT EXISTS XR_Candles (
    xr_candle_id INTEGER PRIMARY KEY DEFAULT nextval('xr_candle_seq'),
    xr_bucket_id INTEGER NOT NULL,
    time_period_start TIMESTAMPTZ NOT NULL,
    time_period_end TIMESTAMPTZ NOT NULL,
    time_open TIMESTAMPTZ,
    time_close TIMESTAMPTZ,
    price_open DECIMAL(20,8),
    price_high DECIMAL(20,8),
    price_low DECIMAL(20,8),
    price_close DECIMAL(20,8),
    volume_traded DECIMAL(20,8),
    trades_count INTEGER,
    delta INTEGER NOT NULL,
    exchange VARCHAR(20),
    pair_type VARCHAR(10),
    price DECIMAL(20,8),
    UNIQUE (xr_bucket_id, time_period_start),
    FOREIGN KEY (xr_bucket_id) REFERENCES XR_Buckets(xr_bucket_id)
);

-- Tabla de precios
CREATE TABLE IF NOT EXISTS XR_Prices (
    to_currency VARCHAR(10) NOT NULL,
    from_currency VARCHAR(10) NOT NULL,
    target_time TIMESTAMPTZ NOT NULL,
    price DECIMAL(20,8) NOT NULL,
    delta INTEGER NOT NULL,
    xr_candle_id INTEGER,
    PRIMARY KEY (to_currency, from_currency, target_time),
    FOREIGN KEY (xr_candle_id) REFERENCES XR_Candles(xr_candle_id)
);

-- Últimos periodos registrados
CREATE TABLE IF NOT EXISTS XR_LastPeriods (
    period_id VARCHAR(20) NOT NULL PRIMARY KEY,
    target_time TIMESTAMPTZ NOT NULL
);
