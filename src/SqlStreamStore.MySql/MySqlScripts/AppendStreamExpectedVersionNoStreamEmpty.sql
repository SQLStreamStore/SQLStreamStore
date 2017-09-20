START TRANSACTION;

INSERT INTO Streams (
            Id,
            IdOriginal,
            Version,
            Position)
     VALUES (
            ?streamId,
            ?streamIdOriginal,
            -1,
            -1);

     SELECT LAST_INSERT_ID()
       INTO @streamIdInternal;
COMMIT;

/* Select CurrentVersion, CurrentPosition */
    SELECT -1, 0, ''

