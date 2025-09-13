-- Database initialization script for financial data pipeline

-- Create tables for historical stock prices
CREATE TABLE IF NOT EXISTS historical_prices (
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DECIMAL(10,2) NOT NULL,
    high DECIMAL(10,2) NOT NULL,
    low DECIMAL(10,2) NOT NULL,
    close DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    adj_close DECIMAL(10,2) NOT NULL,
    batch_id VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol)
);

-- Create table for company fundamentals
CREATE TABLE IF NOT EXISTS company_fundamentals (
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    quarter VARCHAR(5) NOT NULL,
    revenue_mil DECIMAL(12,2),
    eps DECIMAL(6,2),
    pe_ratio DECIMAL(8,2),
    target_price DECIMAL(10,2),
    analyst_rating VARCHAR(10),
    rating_count INTEGER,
    batch_id VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, quarter)
);

-- Create table for real-time stock ticks
CREATE TABLE IF NOT EXISTS realtime_ticks (
    tick_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    volume INTEGER NOT NULL,
    bid DECIMAL(10,2),
    ask DECIMAL(10,2),
    exchange VARCHAR(20),
    price_change_pct DECIMAL(7,4), -- Derived field
    day_high BOOLEAN DEFAULT FALSE, -- Derived field
    day_low BOOLEAN DEFAULT FALSE, -- Derived field
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (timestamp, symbol)
);

-- Create table for trading orders
CREATE TABLE IF NOT EXISTS trading_orders (
    order_id VARCHAR(20) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    order_type VARCHAR(10) NOT NULL,
    side VARCHAR(5) NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2),
    trader_id VARCHAR(20),
    status VARCHAR(15) NOT NULL,
    order_value DECIMAL(14,2), -- Derived field (price * quantity)
    market_impact DECIMAL(7,4), -- Derived field
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create daily summary table for aggregated data
CREATE TABLE IF NOT EXISTS daily_summary (
    date DATE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    avg_price DECIMAL(10,2),
    total_volume BIGINT,
    price_change_pct DECIMAL(7,4),
    num_trades INTEGER,
    source VARCHAR(10) NOT NULL, -- 'batch' or 'stream'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, symbol, source)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_hist_prices_symbol ON historical_prices(symbol);
CREATE INDEX IF NOT EXISTS idx_realtime_ticks_symbol ON realtime_ticks(symbol);
CREATE INDEX IF NOT EXISTS idx_trading_orders_symbol ON trading_orders(symbol);