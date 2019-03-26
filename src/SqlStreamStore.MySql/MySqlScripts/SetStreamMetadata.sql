DROP PROCEDURE IF EXISTS set_stream_metadata;

CREATE PROCEDURE set_stream_metadata(_stream_id CHAR(42),
                                     _metadata_stream_id CHAR(42),
                                     _metadata_stream_id_original CHAR(42),
                                     _max_age INT,
                                     _max_count INT,
                                     _expected_version INT,
                                     _created_utc TIMESTAMP(6),
                                     _metadata_message_message_id BINARY(16),
                                     _metadata_message_type NVARCHAR(128),
                                     _metadata_message_json_data LONGTEXT,
                                     OUT _current_version INT,
                                     OUT _current_position LONG)
BEGIN
    DECLARE _ BOOLEAN;
    IF _created_utc IS NULL
    THEN
        SET _created_utc = UTC_TIMESTAMP(6);
    END IF;

    IF _expected_version = -3
    THEN
        CALL append_to_stream_expected_version_no_stream(
                _metadata_stream_id,
                _metadata_stream_id_original,
                NULL,
                _created_utc,
                _metadata_message_message_id,
                _metadata_message_type,
                _metadata_message_json_data,
                NULL,
                _current_version,
                _current_position,
                _);
    ELSEIF _expected_version = -2
    THEN
        CALL append_to_stream_expected_version_any(
                _metadata_stream_id,
                _metadata_stream_id_original,
                NULL,
                _created_utc,
                _metadata_message_message_id,
                _metadata_message_type,
                _metadata_message_json_data,
                NULL,
                _current_version,
                _current_position,
                _);
    ELSEIF _expected_version = -1
    THEN
        CALL append_to_stream_expected_version_empty_stream(
                _metadata_stream_id,
                _metadata_stream_id_original,
                NULL,
                _created_utc,
                _metadata_message_message_id,
                _metadata_message_type,
                _metadata_message_json_data,
                NULL,
                _current_version,
                _current_position,
                _);
    ELSE
        CALL append_to_stream_expected_version(
                _metadata_stream_id,
                _expected_version,
                _created_utc,
                _metadata_message_message_id,
                _metadata_message_type,
                _metadata_message_json_data,
                NULL,
                _current_version,
                _current_position,
                _);
    END IF;

    UPDATE streams
    SET max_age   = _max_age,
        max_count = _max_count
    WHERE streams.id = _stream_id;

END;