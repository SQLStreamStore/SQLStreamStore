CREATE TYPE NewEvents AS TABLE (
    StreamRevision      INT IDENTITY(0,1)                       NOT NULL,
    Id                  UNIQUEIDENTIFIER    DEFAULT(NEWID())    NULL    ,
    Created             DATETIME            DEFAULT(GETDATE())  NULL    ,
    [Type]              NVARCHAR(128)                           NOT NULL,
    JsonData            NVARCHAR(max)                           NULL    ,
    JsonMetadata        NVARCHAR(max)                           NULL
)
GO
 
INSERT INTO #Events
    (
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES @events
 
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream
    DECLARE @count AS INT;
    DECLARE @streamIdInternal AS INT;
    BEGIN
        INSERT INTO dbo.Streams (Id, IdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, StreamRevision, Id, Created, [Type], JsonData, JsonMetadata)
            SELECT  @streamIdInternal,
                    StreamRevision,
                    Id,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
                FROM #Events
 
    END
    SELECT @streamIdInternal
COMMIT TRANSACTION CreateStream
 
DROP TABLE #Events