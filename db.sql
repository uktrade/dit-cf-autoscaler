DROP TABLE IF EXISTS metrics;
DROP TABLE IF EXISTS actions;

CREATE TABLE metrics (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP,
    app             VARCHAR(100),
    space           VARCHAR(100),
    instance_count  SMALLINT,
    average_cpu     REAL
);

CREATE UNIQUE INDEX metric_id_unique_idx ON metrics (id);
CREATE INDEX metrics_app_space_timestamp_idx ON metrics (app, space, timestamp);

CREATE TABLE actions (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP,
    app             VARCHAR(100),
    space           VARCHAR(100),
    instances       SMALLINT
);

CREATE UNIQUE INDEX actions_id_unique_idx ON actions (id);
CREATE INDEX actions_app_space_timestamp_idx ON actios (app, space, timestamp);

