EXISTS (SELECT * FROM streams WHERE streams.id == @streamId)
BEGIN;
    /* Create in-memory temp table for variables */
    PRAGMA temp_store = 2;
    CREATE TEMP TABLE _Variables(Name TEXT PRIMARY KEY, RealValue REAL, IntegerValue INTEGER, BlobValue BLOB, TextValue TEXT);

    /* Declaring a variable */
    INSERT INTO _Variables (Name) VALUES ('_stream_id_internal');
    INSERT INTO _Variables (Name) VALUES ('_max_count');

    /* Assigning a variable (pick the right storage class) */
    UPDATE _Variables SET IntegerValue = (SELECT streams.id_internal) WHERE Name = '_stream_id_internal';
    UPDATE _Variables SET IntegerValue = (SELECT streams.max_count) WHERE Name = '_max_count';

    /* Getting variable value (use within expression) */
    SELECT messages.message_id
    FROM messages
    WHERE messages.stream_id_internal = ((SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_stream_id_internal' LIMIT 1))
        AND messages.message_id NOT IN (SELECT messages.message_id
                                        FROM messages
                                        WHERE messages.stream_id_internal = (SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_stream_id_internal' LIMIT 1))
                                            ORDER BY messages.stream_version desc
                                            LIMIT (SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_max_count' LIMIT 1))
    ORDER BY messages.stream_version;

    DROP TABLE _Variables;
END;