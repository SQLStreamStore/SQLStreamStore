START TRANSACTION;

INSERT INTO Streams (
            Id,
            IdOriginal)
     SELECT ?streamId,
            ?streamIdOriginal
       FROM DUAL
      WHERE NOT EXISTS (
     SELECT 1
       FROM Streams
      WHERE Streams.Id = ?streamId
       LOCK IN SHARE MODE);
COMMIT;

START TRANSACTION;
     SELECT Streams.IdInternal,
            Streams.Version
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal,
            @latestStreamVersion;
COMMIT;

/* Select CurrentVersion, CurrentPosition */
     SELECT -1,
            0,
            ''
