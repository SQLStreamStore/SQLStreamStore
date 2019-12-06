DROP PROCEDURE IF EXISTS append_to_stream_expected_version_any;

CREATE PROCEDURE append_to_stream_expected_version_any(_stream_id CHAR(42),
                                                       _stream_id_original VARCHAR(1000),
                                                       _metadata_stream_id CHAR(42),
                                                       _created_utc TIMESTAMP(6),
                                                       _message_id BINARY(16),
                                                       _type NVARCHAR(128),
                                                       _json_data LONGTEXT,
                                                       _json_metadata LONGTEXT,
                                                       OUT _current_version INT,
                                                       OUT _current_position BIGINT,
                                                       OUT _message_exists BOOLEAN)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _stream_version INT;
    DECLARE _max_age INT;
    DECLARE _max_count INT;

    SELECT streams.id_internal INTO _stream_id_internal
    FROM streams
    WHERE streams.id = _stream_id
    FOR UPDATE;

    IF _stream_id_internal IS NULL THEN
        CALL get_stream_metadata(_metadata_stream_id, _max_age, _max_count);

        INSERT INTO streams (id, id_original, max_age, max_count)
        VALUES (_stream_id, _stream_id_original, _max_age, _max_count);

        SET _stream_id_internal := LAST_INSERT_ID();
    END IF;

    SELECT COUNT(*) > 0,
           messages.stream_version
           INTO _message_exists,
               _stream_version
    FROM messages
    WHERE messages.stream_id_internal = _stream_id_internal
      AND messages.message_id = _message_id;

    IF NOT _message_exists
    THEN
        IF (_created_utc IS NULL)
        THEN
            SET _created_utc := UTC_TIMESTAMP(6);
        END IF;

        SELECT streams.version INTO _stream_version
        FROM streams
        WHERE streams.id_internal = _stream_id_internal;

        INSERT INTO messages (stream_id_internal,
                              stream_version,
                              message_id,
                              created_utc,
                              type,
                              json_data,
                              json_metadata)
        VALUES (_stream_id_internal,
                _stream_version + 1,
                _message_id,
                _created_utc,
                _type,
                _json_data,
                _json_metadata);

        SET _current_version := _stream_version + 1;

        SET _current_position := LAST_INSERT_ID();

        UPDATE streams
        SET streams.version  = _current_version,
            streams.position = _current_position
        WHERE streams.id_internal = _stream_id_internal;

        SELECT _current_version;
    ELSE
        SELECT streams.version, streams.position INTO _current_version, _current_position
        FROM streams
        WHERE streams.id_internal = _stream_id_internal;

        SELECT _stream_version;
    END IF;

END;
