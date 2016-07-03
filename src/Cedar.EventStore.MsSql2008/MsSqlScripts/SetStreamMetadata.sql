BEGIN TRANSACTION SetMetadata;

    UPDATE dbo.StreamMetadata with (serializable)
       SET MaxAge = @maxAge, MaxCount = @maxCount
     WHERE StreamId = @streamId
        IF @@ROWCOUNT=0
        BEGIN
            INSERT INTO dbo.StreamMetadata (StreamId, MaxAge, MaxCount)
            VALUES (@streamId, @maxAge, @maxCount);
        END

COMMIT TRANSACTION SetMetadata;
