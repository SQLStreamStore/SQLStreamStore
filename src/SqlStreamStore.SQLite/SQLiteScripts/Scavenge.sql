IF EXISTS (SELECT * FROM __schema__.streams WHERE __schema__.streams.id == @streamId)
BEGIN;
    /* Create in-memory temp table for variables */
    PRAGMA temp_store = 2;
    CREATE TEMP TABLE _Variables(Name TEXT PRIMARY KEY, RealValue REAL, IntegerValue INTEGER, BlobValue BLOB, TextValue TEXT);

    /* Declaring a variable */
    INSERT INTO _Variables (Name) VALUES ('_stream_id_internal');
    INSERT INTO _Variables (Name) VALUES ('_max_count');

    /* Assigning a variable (pick the right storage class) */
    UPDATE _Variables SET IntegerValue = (SELECT __schema__.streams.id_internal) WHERE Name = '_stream_id_internal';
    UPDATE _Variables SET IntegerValue = (SELECT __schema__.streams.max_count) WHERE Name = '_max_count';

    /* Getting variable value (use within expression) */
    SELECT __schema__.messages.message_id
    FROM __schema__.messages
    WHERE __schema__.messages.stream_id_internal = ((SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_stream_id_internal' LIMIT 1))
        AND __schema__.messages.message_id NOT IN (SELECT __schema__.messages.message_id
                                        FROM __schema__.messages
                                        WHERE __schema__.messages.stream_id_internal = (SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_stream_id_internal' LIMIT 1))
                                            ORDER BY __schema__.messages.stream_version desc
                                            LIMIT (SELECT coalesce(RealValue, IntegerValue, BlobValue, TextValue) FROM _Variables WHERE Name = '_max_count' LIMIT 1))
    ORDER BY __schema__.messages.stream_version;

    DROP TABLE _Variables;
END;