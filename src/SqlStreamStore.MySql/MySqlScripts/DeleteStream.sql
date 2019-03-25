DROP PROCEDURE IF EXISTS delete_stream;

CREATE PROCEDURE delete_stream(_stream_id CHAR(42),
                               _expected_version INT,
                               _created_utc TIMESTAMP(6),
                               _deleted_stream_id CHAR(42),
                               _deleted_stream_id_original NVARCHAR(1000),
                               _deleted_metadata_stream_id CHAR(42),
                               _deleted_stream_message_message_id BINARY(16),
                               _deleted_stream_message_type NVARCHAR(128),
                               _deleted_stream_message_json_data LONGTEXT)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _latest_stream_version INT;
    DECLARE _affected INT;
    DECLARE _ INT;
    DECLARE __ BIGINT;

    BEGIN
        IF _created_utc IS NULL
        THEN
            SET _created_utc = UTC_TIMESTAMP(6);
        END IF;
        SELECT streams.id_internal INTO _stream_id_internal
        FROM streams
        WHERE streams.id = _stream_id;

        IF _expected_version = -1 /* ExpectedVersion.NoStream */
        THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'WrongExpectedVersion';
        ELSEIF _expected_version >= 0 /* ExpectedVersion */
        THEN
            IF _stream_id_internal IS NULL
            THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'WrongExpectedVersion';
            END IF;

            SELECT messages.stream_version INTO _latest_stream_version
            FROM messages
            WHERE messages.stream_id_internal = _stream_id_internal
            ORDER BY messages.position DESC
            LIMIT 1;

            IF _latest_stream_version != _expected_version
            THEN
                SIGNAL SQLSTATE '45000'
                    SET MESSAGE_TEXT = 'WrongExpectedVersion';
            END IF;
        END IF;

        DELETE FROM messages WHERE messages.stream_id_internal = _stream_id_internal;

        DELETE FROM streams WHERE streams.id = _stream_id;

        SELECT ROW_COUNT() INTO _affected;

        IF _affected > 0
        THEN
            CALL append_to_stream_expected_version_any
                (
                    _deleted_stream_id,
                    _deleted_stream_id_original,
                    _deleted_metadata_stream_id,
                    _created_utc,
                    _deleted_stream_message_message_id,
                    _deleted_stream_message_type,
                    _deleted_stream_message_json_data,
                    NULL,
                    _,
                    __);

        END IF;
    END;

END;
