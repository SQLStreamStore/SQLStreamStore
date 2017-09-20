START TRANSACTION;

     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

DELETE FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
        AND Messages.Id = ?eventId;

     SELECT ROW_COUNT();

COMMIT;