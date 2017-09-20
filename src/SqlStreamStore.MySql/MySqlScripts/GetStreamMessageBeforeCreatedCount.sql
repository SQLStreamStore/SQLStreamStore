     SELECT Streams.IdInternal
       FROM Streams
      WHERE Streams.Id = ?streamId
       INTO @streamIdInternal;

     SELECT COUNT(*)
       FROM Messages
      WHERE Messages.StreamIdInternal = @streamIdInternal
        AND Messages.Created < ?created;
