START TRANSACTION;

INSERT INTO Streams (
            Id,
            IdOriginal,
            Version,
            Position)
     SELECT ?streamId,
            ?streamIdOriginal,
            -1, 
            0
       FROM DUAL
      WHERE NOT EXISTS (
     SELECT 1
       FROM Streams
      WHERE Streams.Id = ?streamId
       LOCK IN SHARE MODE);

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
