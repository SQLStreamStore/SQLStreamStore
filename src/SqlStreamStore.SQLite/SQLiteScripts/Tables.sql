CREATE TABLE IF NOT EXISTS streams(
    id              text    NOT NULL                UNIQUE,
    id_original     text    NOT NULL,
    id_internal     integer NOT NULL PRIMARY KEY    AUTOINCREMENT,
    version         integer NOT NULL DEFAULT -1     CHECK (version >= -1),
    position        integer NOT NULL DEFAULT -1,
    max_age         integer NULL,
    max_count       integer NULL
);

CREATE INDEX IF NOT EXISTS ix_id_original
  ON streams (id_original);

CREATE TABLE IF NOT EXISTS messages (
    stream_id_internal      integer     NOT NULL,
    stream_version          integer     NOT NULL    CHECK (stream_version >= 0),
    [position]              integer     NOT NULL PRIMARY KEY   AUTOINCREMENT,
    message_id              CHAR(36)    NOT NULL,
    created_utc             DATETIME    NOT NULL,
    type                    CHAR(128)   NOT NULL,
    json_data               text        NOT NULL,
    json_metadata           text,
    FOREIGN KEY (stream_id_internal) REFERENCES streams (id_internal),
    UNIQUE (stream_id_internal, stream_version)
    UNIQUE (stream_id_internal, message_id)
);
