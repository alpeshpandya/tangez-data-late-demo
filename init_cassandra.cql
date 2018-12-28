CREATE KEYSPACE tangez_staging WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE tangez_staging.meter_data(
    meter_data_id TEXT PRIMARY KEY,
    received_ts TIMESTAMP,
    sync_bit int,
    preamble TEXT,
    ert_id_ms_bits TEXT,
    reserved TEXT,
    physical_tamper TEXT,
    ert_type TEXT,
    encoder_tamper TEXT,
    consumption_data int,
    ert_id_ls_bits TEXT,
    checksum TEXT
);

CREATE TABLE tangez_staging.customer_data(
    name TEXT,
    address TEXT,
    ert_id TEXT PRIMARY KEY
);

CREATE KEYSPACE tangez_transformed WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE tangez_transformed.meter_data(
    meter_data_id TEXT PRIMARY KEY,
    received_ts TIMESTAMP,
    sync_bit int,
    preamble TEXT,
    ert_id_ms_bits TEXT,
    reserved TEXT,
    physical_tamper TEXT,
    ert_type TEXT,
    encoder_tamper TEXT,
    consumption_data int,
    ert_id_ls_bits TEXT,
    checksum TEXT,
    ert_id TEXT
);

CREATE TABLE tangez_transformed.customer_data(
    name TEXT,
    address TEXT,
    ert_id TEXT PRIMARY KEY
);

CREATE TABLE tangez_transformed.meter_customer_data(
    meter_data_id TEXT PRIMARY KEY,
    received_ts TIMESTAMP,
    sync_bit int,
    preamble TEXT,
    ert_id_ms_bits TEXT,
    reserved TEXT,
    physical_tamper TEXT,
    ert_type TEXT,
    encoder_tamper TEXT,
    consumption_data int,
    ert_id_ls_bits TEXT,
    checksum TEXT,
    ert_id TEXT,
    name TEXT,
    address TEXT,
);

CREATE KEYSPACE tangez_data_marts WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE tangez_data_marts.consumption_15_minutes_sum(
    ert_id TEXT PRIMARY KEY,
    received_ts TIMESTAMP,
    consumption_data_sum int
);

CREATE TABLE tangez_data_marts.consumption_1_hour_sum(
    ert_id TEXT PRIMARY KEY,
    start_of_hour_time TIMESTAMP,
    consumption_data_sum int
);

CREATE TABLE tangez_data_marts.consumption_hourly_by_customer_avg(
    ert_id TEXT PRIMARY KEY,
    start_of_hour_time TIMESTAMP,
    consumption_data_avg int
);

CREATE TABLE tangez_data_marts.consumption_anomaly(
    ert_id TEXT PRIMARY KEY,
    received_ts TIMESTAMP,
    consumption_data int,
    deviation int
);