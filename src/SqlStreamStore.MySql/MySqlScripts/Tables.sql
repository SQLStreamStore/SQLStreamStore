CREATE TABLE IF NOT EXISTS streams
(
    id          CHAR(42)       NOT NULL,
    id_original NVARCHAR(1000) NOT NULL,
    id_internal INT            NOT NULL AUTO_INCREMENT,
    version     INT            NOT NULL DEFAULT -1,
    position    BIGINT         NOT NULL DEFAULT -1,
    max_age     INT            NULL,
    max_count   INT            NULL,
    CONSTRAINT pk_streams PRIMARY KEY (id_internal),
    CONSTRAINT uq_streams_id UNIQUE KEY (id),
    CONSTRAINT ck_version_gte_negative_one CHECK (version >= -1)
) ENGINE = InnoDB
    COMMENT '{ "version": 1 }';

CREATE TABLE IF NOT EXISTS messages
(
    stream_id_internal INT           NOT NULL,
    stream_version     INT           NOT NULL,
    position           BIGINT        NOT NULL AUTO_INCREMENT,
    message_id         BINARY(16)    NOT NULL,
    created_utc        TIMESTAMP     NOT NULL,
    type               NVARCHAR(128) NOT NULL,
    json_data          LONGTEXT      NOT NULL,
    json_metadata      LONGTEXT,
    CONSTRAINT pk_messages PRIMARY KEY (position),
    CONSTRAINT fk_messages_stream FOREIGN KEY (stream_id_internal) REFERENCES streams (id_internal),
    CONSTRAINT uq_messages_stream_id_internal_and_stream_version UNIQUE KEY (stream_id_internal, stream_version),
    CONSTRAINT uq_stream_id_internal_and_message_id UNIQUE KEY (stream_id_internal, message_id),
    CONSTRAINT ck_stream_version_gte_zero CHECK (stream_version >= 0)
) ENGINE = InnoDB;
