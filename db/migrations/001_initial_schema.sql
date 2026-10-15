-- 001: the initial data model.
-- Every reading is stored once per (device_id, epoch, seq). The current state per device is kept separately.

CREATE TABLE gateways (
    gateway_id      text PRIMARY KEY,
    first_seen      timestamptz NOT NULL,
    -- NULL until the first status message: a gateway can appear through its readings before its status arrives.
    last_status     text CHECK (last_status IN ('online', 'offline')),
    last_status_at  timestamptz,
    buffered        bigint,
    dropped         bigint,
    mode            text
);

-- Each device is bound to the gateway it first appeared under (D6).
CREATE TABLE devices (
    device_id   text PRIMARY KEY,
    gateway_id  text NOT NULL REFERENCES gateways (gateway_id),
    kind        text NOT NULL CHECK (kind IN ('Inverter', 'Battery', 'Charger', 'GridMeter')),
    first_seen  timestamptz NOT NULL
);
CREATE INDEX devices_gateway_id_idx ON devices (gateway_id);

-- The history. The primary key is the dedup key (D4). There's no foreign key to devices, which keeps the hot insert
-- path to a single index check; ingest registers the device in the same transaction.
CREATE TABLE readings (
    device_id        text             NOT NULL,
    epoch            bigint           NOT NULL,
    seq              bigint           NOT NULL,
    measured_at      timestamptz      NOT NULL,
    received_at      timestamptz      NOT NULL,
    kind             text             NOT NULL,
    pv_power_w       double precision,
    battery_power_w  double precision,
    grid_power_w     double precision,
    ev_power_w       double precision,
    energy_wh        double precision,
    soc_percent      double precision,
    charge_limit_a   double precision,
    PRIMARY KEY (device_id, epoch, seq)
);
CREATE INDEX readings_device_measured_idx ON readings (device_id, measured_at);

-- The latest reading per device: what "now" looks like. Slice 2 makes it move forward only (D4).
CREATE TABLE device_state (
    device_id        text PRIMARY KEY,
    epoch            bigint           NOT NULL,
    seq              bigint           NOT NULL,
    measured_at      timestamptz      NOT NULL,
    received_at      timestamptz      NOT NULL,
    kind             text             NOT NULL,
    pv_power_w       double precision,
    battery_power_w  double precision,
    grid_power_w     double precision,
    ev_power_w       double precision,
    energy_wh        double precision,
    soc_percent      double precision,
    charge_limit_a   double precision
);

-- Messages that can't be stored, kept byte for byte with the reason (D7).
CREATE TABLE dead_letters (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    topic        text        NOT NULL,
    payload      bytea       NOT NULL,
    reason       text        NOT NULL,
    detail       text,
    received_at  timestamptz NOT NULL
);
CREATE INDEX dead_letters_received_at_idx ON dead_letters (received_at);
