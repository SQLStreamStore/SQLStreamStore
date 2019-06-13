DROP PROCEDURE IF EXISTS list_streams;

CREATE PROCEDURE list_streams(_max_count INT,
                              _after_id_internal INT)
BEGIN

    SELECT streams.id_original, streams.id_internal
    FROM streams
    WHERE streams.id_internal > IFNULL(_after_id_internal, -1)
    ORDER BY streams.id_internal ASC
    LIMIT _max_count;

END;
