CREATE TABLE IF NOT EXISTS streams(
    id              text    NOT NULL                UNIQUE,
    id_original     text    NOT NULL,
    id_internal     int     NOT NULL PRIMARY KEY    AUTOINCREMENT,
    version         int     NOT NULL DEFAULT -1     CHECK (version >= -1),
    position        int     NOT NULL DEFAULT -1,
    max_age         int     NULL,
    max_count       int     NULL
)

CREATE INDEX IF NOT EXISTS ix_id_original
  ON streams (id_original)

CREATE TABLE IF NOT EXISTS messages (
    stream_id_internal      INT         NOT NULL,
    stream_version          INT         NOT NULL    CHECK (version >= 0),
    position                INT         NOT NULL PRIMARY KEY   AUTOINCREMENT,
    message_id              CHAR(36)    NOT NULL,
    created_utc             DATETIME    NOT NULL,
    type                    CHAR(128)   NOT NULL,
    json_data               text        NOT NULL,
    json_metadata           text,
    FOREIGN KEY (stream_id_internal) REFERENCES streams(id_internal),
    UNIQUE (stream_id_internal, stream_version)
    UNIQUE (stream_id_internal, message_id)
)
