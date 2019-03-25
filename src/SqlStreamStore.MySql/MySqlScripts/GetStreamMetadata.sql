DROP PROCEDURE IF EXISTS get_stream_metadata;

CREATE PROCEDURE get_stream_metadata(_metadata_stream_id CHAR(42),
                                     OUT _max_age INT,
                                     OUT _max_count INT)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _json_data LONGTEXT;
    DECLARE _start_index INT;
    DECLARE _last_index INT;
    DECLARE _metadata_value VARCHAR(10);

    SELECT streams.id_internal INTO _stream_id_internal
    FROM streams
    WHERE streams.id = _metadata_stream_id;

    IF _stream_id_internal IS NOT NULL
    THEN

        SELECT messages.json_data INTO _json_data
        FROM messages
        WHERE messages.stream_id_internal = _stream_id_internal
        ORDER BY messages.stream_version DESC
        LIMIT 1;

        IF _json_data IS NOT NULL
        THEN

            SET _start_index := INSTR(_json_data, '"MaxAge":') + 9;
            SET _last_index := INSTR(SUBSTRING(_json_data, _start_index), ',') + _start_index - 1;
            SET _metadata_value := SUBSTRING(
                    _json_data,
                    _start_index,
                    _last_index - _start_index);
            SET _max_age :=
                    IF((_metadata_value = 'null' OR _metadata_value = ''), NULL, CONVERT(_metadata_value, SIGNED INT));

            SET _start_index := INSTR(_json_data, '"MaxCount":') + 11;
            SET _last_index := INSTR(SUBSTRING(_json_data, _start_index), ',') + _start_index - 1;
            SET _metadata_value := SUBSTRING(
                    _json_data,
                    _start_index,
                    _last_index - _start_index);
            SET _max_count :=
                    IF((_metadata_value = 'null' OR _metadata_value = ''), NULL, CONVERT(_metadata_value, SIGNED INT));

        END IF;

    END IF;
END;