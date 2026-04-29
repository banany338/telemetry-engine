CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS raw_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    endpoint VARCHAR(255),
    status_code INTEGER,
    response_time_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_raw_logs_timestamp ON raw_logs (timestamp);

CREATE TABLE IF NOT EXISTS anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    anomaly_type VARCHAR(100) NOT NULL,
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_anomalies_timestamp ON anomalies (timestamp);
