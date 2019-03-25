DROP PROCEDURE IF EXISTS append_to_stream_expected_version;

CREATE PROCEDURE append_to_stream_expected_version(_stream_id CHAR(42),
                                                   _expected_version INT,
                                                   _created_utc TIMESTAMP(6),
                                                   _message_id BINARY(16),
                                                   _type NVARCHAR(128),
                                                   _json_data LONGTEXT,
                                                   _json_metadata LONGTEXT,
                                                   OUT _current_version INT,
                                                   OUT _current_position BIGINT)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _message_exists BOOLEAN;

    SELECT streams.id_internal INTO _stream_id_internal
    FROM streams
    WHERE streams.id = _stream_id;

    IF (SELECT streams.version
        FROM streams
        WHERE streams.id_internal = _stream_id_internal) < _expected_version
    THEN
        SIGNAL SQLSTATE '23000'
            SET MESSAGE_TEXT = 'WrongExpectedVersion';
    END IF;

    SET _message_exists := (SELECT COUNT(*)
                            FROM messages
                            WHERE stream_id_internal = _stream_id_internal
                              AND stream_version = _expected_version + 1
                              AND message_id = _message_id) > 0;

    IF NOT _message_exists
    THEN
        IF (_created_utc IS NULL)
        THEN
            SET _created_utc := UTC_TIMESTAMP(6);
        END IF;

        INSERT INTO messages (stream_id_internal,
                              stream_version,
                              message_id,
                              created_utc,
                              type,
                              json_data,
                              json_metadata)
        VALUES (_stream_id_internal,
                _expected_version + 1,
                _message_id,
                _created_utc,
                _type,
                _json_data,
                _json_metadata);

        SET _current_position := LAST_INSERT_ID();

        SET _current_version := _expected_version + 1;

        UPDATE streams
        SET version  = _expected_version + 1,
            position = _current_position
        WHERE id_internal = _stream_id_internal;
    ELSE
        SELECT streams.version, streams.position INTO _current_version, _current_position
        FROM streams
        WHERE streams.id_internal = _stream_id_internal;
    END IF;

    SELECT _expected_version + 1;
END;
