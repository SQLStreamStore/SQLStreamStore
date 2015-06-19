--SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
--BEGIN TRANSACTION CreateStream;
--    integer stream_id_internal;
--    BEGIN
--        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamIdOriginal);
--        SELECT @streamIdInternal = SCOPE_IDENTITY();

--        INSERT INTO dbo.Events (StreamIdInternal, StreamVersion, Id, Created, [Type], JsonData, JsonMetadata)
--             SELECT @streamIdInternal,
--                    StreamVersion,
--                    Id,
--                    Created,
--                    [Type],
--                    JsonData,
--                    JsonMetadata
--               FROM @events;
 
--    END;
--    SELECT @streamIdInternal;
--COMMIT TRANSACTION CreateStream;
