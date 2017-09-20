     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

     SELECT Messages.JsonData
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
        AND Messages.StreamVersion = ?streamVersion;
