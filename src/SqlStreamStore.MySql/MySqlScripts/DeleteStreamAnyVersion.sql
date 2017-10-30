START TRANSACTION;
     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

DELETE FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal;

DELETE FROM Streams
      WHERE Streams.IdInternal = @streamIdInternal;
     SELECT ROW_COUNT();

COMMIT;