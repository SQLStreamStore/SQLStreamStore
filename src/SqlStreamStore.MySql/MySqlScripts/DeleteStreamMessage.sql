DROP PROCEDURE IF EXISTS delete_stream_message;

CREATE PROCEDURE delete_stream_message(_stream_id CHAR(42),
                                       _message_id BINARY(16),
                                       _deleted_stream_id CHAR(42),
                                       _deleted_stream_id_original NVARCHAR(1000),
                                       _deleted_metadata_stream_id CHAR(42),
                                       _created_utc TIMESTAMP(6),
                                       _deleted_message_message_id BINARY(16),
                                       _deleted_message_type NVARCHAR(128),
                                       _deleted_message_json_data LONGTEXT)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _ INT;
    DECLARE __ BIGINT;
    DECLARE ___ BOOLEAN;

    SELECT streams.id_internal INTO _stream_id_internal
    FROM streams
    WHERE streams.id = _stream_id;

    DELETE
    FROM messages
    WHERE messages.stream_id_internal = _stream_id_internal
      AND messages.message_id = _message_id;

    IF ROW_COUNT() > 0
    THEN
        IF _created_utc IS NULL THEN
            SET _created_utc = UTC_TIMESTAMP(6);
        END IF;

        CALL append_to_stream_expected_version_any(
                _deleted_stream_id,
                _deleted_stream_id_original,
                _deleted_metadata_stream_id,
                _created_utc,
                _deleted_message_message_id,
                _deleted_message_type,
                _deleted_message_json_data,
                NULL,
                _,
                __,
                ___);
    END IF;
END;