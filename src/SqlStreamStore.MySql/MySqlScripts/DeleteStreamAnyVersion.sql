START TRANSACTION;
     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

     DELETE Messages
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal;

     DELETE Streams
       FROM Streams
      WHERE Streams.IdInternal = @streamIdInternal;

     SELECT ROW_COUNT();
COMMIT;