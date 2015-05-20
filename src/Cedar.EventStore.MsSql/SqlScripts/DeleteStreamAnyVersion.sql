BEGIN TRANSACTION DeleteStream
        DECLARE @streamIdInternal AS INT

         SELECT @streamIdInternal = Streams.IdInternal
           FROM Streams
          WHERE Streams.Id = @streamId;

    DELETE FROM Events
          WHERE Events.StreamIdInternal = @streamIdInternal;
       
         UPDATE Streams
            SET IsDeleted = '1'
          WHERE Streams.Id = @streamId;
COMMIT TRANSACTION DeleteStream