     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

     SELECT Messages.StreamVersion
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal AND Messages.Id = ?messageId
      LIMIT 1;