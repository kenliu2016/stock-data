-- 美国股票表
CREATE TABLE IF NOT EXISTS us_stocks (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100),
    industry VARCHAR(100),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 中国A股表
CREATE TABLE IF NOT EXISTS cn_stocks (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100),
    industry VARCHAR(100),
    market VARCHAR(20),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 香港股票表
CREATE TABLE IF NOT EXISTS hk_stocks (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) NOT NULL UNIQUE,
    name VARCHAR(100),
    industry VARCHAR(100),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 股票分钟数据表
CREATE TABLE IF NOT EXISTS stock_minute_data (
    code VARCHAR(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, datetime)
);

-- 创建A股实时数据表
CREATE TABLE IF NOT EXISTS cn_data_realtime (
    code VARCHAR(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, datetime)
);

-- 创建港股实时数据表
CREATE TABLE IF NOT EXISTS hk_data_realtime (
    code VARCHAR(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, datetime)
);

-- 创建美股实时数据表
CREATE TABLE IF NOT EXISTS us_data_realtime (
    code VARCHAR(50) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    open NUMERIC(10,4),
    high NUMERIC(10,4),
    low NUMERIC(10,4),
    close NUMERIC(10,4),
    volume BIGINT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (code, datetime)
);

-- 增加索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_cn_data_realtime_code ON cn_data_realtime (code);
CREATE INDEX IF NOT EXISTS idx_cn_data_realtime_datetime ON cn_data_realtime (datetime);

CREATE INDEX IF NOT EXISTS idx_hk_data_realtime_code ON hk_data_realtime (code);
CREATE INDEX IF NOT EXISTS idx_hk_data_realtime_datetime ON hk_data_realtime (datetime);

CREATE INDEX IF NOT EXISTS idx_us_data_realtime_code ON us_data_realtime (code);
CREATE INDEX IF NOT EXISTS idx_us_data_realtime_datetime ON us_data_realtime (datetime);

-- 创建schema_updates表用于记录数据库更新历史
CREATE TABLE IF NOT EXISTS schema_updates (
    id SERIAL PRIMARY KEY,
    description TEXT NOT NULL,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 记录创建时间
INSERT INTO schema_updates (description, update_time) 
VALUES ('Created all database tables including stock codes, minute data and realtime data tables', CURRENT_TIMESTAMP);