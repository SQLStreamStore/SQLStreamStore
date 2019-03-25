DROP PROCEDURE IF EXISTS scavenge;

CREATE PROCEDURE scavenge(
    _stream_id CHAR(42)
)
BEGIN
    DECLARE _stream_id_internal INT;
    DECLARE _max_count INT;
    DECLARE _limit INT;

    SELECT streams.id_internal,
           streams.max_count
           INTO _stream_id_internal, _max_count
    FROM streams
    WHERE streams.id = _stream_id;

    IF (_max_count IS NOT NULL)
    THEN
        SELECT COUNT(*) - _max_count INTO _limit
        FROM messages
        WHERE messages.stream_id_internal = _stream_id_internal;

        IF (_limit > 0) THEN
            SELECT messages.message_id
            FROM messages
            WHERE messages.stream_id_internal = _stream_id_internal
            ORDER BY messages.stream_version ASC
            LIMIT _limit;
        ELSE
            SELECT NULL FROM messages WHERE FALSE;
        END IF;
    ELSE
        SELECT NULL FROM messages WHERE FALSE;
    END IF;
END;
