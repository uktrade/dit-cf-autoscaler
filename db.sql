DROP TABLE IF EXISTS metrics;
DROP TABLE IF EXISTS actions;

CREATE TABLE metrics (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP,
    metric          VARCHAR(100),
    app             VARCHAR(100),
    space           VARCHAR(100),
    instance        SMALLINT,
    value           REAL
);

CREATE TABLE actions (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMP,
    app             VARCHAR(100),
    space           VARCHAR(100),
    instances       SMALLINT
);
