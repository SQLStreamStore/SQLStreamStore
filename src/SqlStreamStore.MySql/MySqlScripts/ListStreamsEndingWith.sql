DROP PROCEDURE IF EXISTS list_streams_ending_with;

CREATE PROCEDURE list_streams_ending_with(_pattern VARCHAR(1000),
                                          _max_count INT,
                                          _after_id_internal INT)
BEGIN

    SELECT streams.id_original, streams.id_internal
    FROM streams
    WHERE streams.id_internal > IFNULL(_after_id_internal, -1)
      AND REVERSE(streams.id_original) LIKE CONCAT(REVERSE(_pattern), '%')
    ORDER BY streams.id_internal ASC
    LIMIT _max_count;

END;