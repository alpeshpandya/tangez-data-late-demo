CREATE KEYSPACE tangez_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE tangez_demo.meter_data(
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

CREATE TABLE tangez_demo.customer_data(
    name TEXT,
    address TEXT,
    ert_id TEXT PRIMARY KEY
);
