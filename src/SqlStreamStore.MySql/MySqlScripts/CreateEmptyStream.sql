DROP PROCEDURE IF EXISTS create_empty_stream;

CREATE PROCEDURE create_empty_stream(_stream_id CHAR(42),
                                     _stream_id_original VARCHAR(1000),
                                     _metadata_stream_id CHAR(42),
                                     _expected_version INT)
BEGIN
    DECLARE _max_age INT;
    DECLARE _max_count INT;

    IF (_expected_version = -1) /* ExpectedVersion.EmptyStream */
    THEN
        IF (SELECT streams.version
            FROM streams
            WHERE streams.id = _stream_id) >= 0
        THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'WrongExpectedVersion';
        END IF;
    ELSEIF (_expected_version = -3) /* ExpectedVersion.NoStream */
    THEN
        IF (SELECT COUNT(*)
            FROM streams
            WHERE streams.id = _stream_id) > 0
        THEN
            SIGNAL SQLSTATE '45000'
                SET MESSAGE_TEXT = 'WrongExpectedVersion';
        END IF;
    END IF;

    IF (SELECT COUNT(*) FROM streams WHERE streams.id = _stream_id) = 0
    THEN
        CALL get_stream_metadata(_metadata_stream_id, _max_age, _max_count);

        INSERT INTO streams (id, id_original, max_age, max_count)
        VALUES (_stream_id, _stream_id_original, _max_age, _max_count);
    END IF;

    SELECT streams.version, streams.position
    FROM streams
    WHERE streams.id = _stream_id;
END;
