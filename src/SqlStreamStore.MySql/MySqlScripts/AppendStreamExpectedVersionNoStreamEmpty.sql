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
            0);
COMMIT;

/* Select CurrentVersion, CurrentPosition */
    SELECT -1, 0, ''

