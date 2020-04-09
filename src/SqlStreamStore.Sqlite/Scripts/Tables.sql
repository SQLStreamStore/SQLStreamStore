CREATE TABLE IF NOT EXISTS streams(
    id              text    NOT NULL                            UNIQUE,
    id_original     text    NOT NULL,
    id_internal     integer NOT NULL PRIMARY KEY                AUTOINCREMENT,
    version         integer NOT NULL DEFAULT -1                 CHECK (version >= -1),
    position        integer NOT NULL DEFAULT -1,
    max_age         integer NULL,
    max_count       integer NULL
);

CREATE INDEX IF NOT EXISTS ix_id_original
  ON streams (id_original);

CREATE TABLE IF NOT EXISTS messages (
    event_id                CHAR(36)    NOT NULL,
    stream_id_internal      integer     NOT NULL,
    stream_version          integer     NOT NULL                CHECK (stream_version >= 0),
    [position]              integer     NOT NULL PRIMARY KEY    AUTOINCREMENT,
    created_utc             DATETIME    NOT NULL,
    type                    CHAR(128)   NOT NULL,
    json_data               text        NOT NULL,
    json_metadata           text,
    FOREIGN KEY (stream_id_internal) REFERENCES streams (id_internal),
    UNIQUE (stream_id_internal, stream_version)
    UNIQUE (stream_id_internal, event_id)
);


INSERT OR IGNORE INTO streams (id, id_original, [version], [position])
VALUES ('$$INIT-STREAM', 'INIT-STREAM', 0, 0);

INSERT OR IGNORE INTO messages (event_id, stream_id_internal, stream_version, [position], created_utc, [type], json_data)
VALUES ('00000000-0000-0000-0000-000000000000', last_insert_rowid(), 0, -1, datetime('now'), '$META', '');
