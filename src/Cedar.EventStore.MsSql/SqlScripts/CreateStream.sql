CREATE TABLE #Events (
    EventId         UNIQUEIDENTIFIER    default(NEWID())    NULL        ,
    SequenceNumber  INT IDENTITY(0,1)                       NOT NULL    ,
    Created         DATETIME            default(GETDATE())  NULL        ,
    [Type]          NVARCHAR(128)                           NOT NULL    ,
    JsonData        NVARCHAR(max)                           NULL        ,
    JsonMetadata    NVARCHAR(max)                           NULL
)
 
INSERT INTO #Events
    (
        [Type]          ,
        JsonData        ,
        JsonMetadata
    ) VALUES
    ('type1',    '\"data1\"',    '\"meta1\"'),
    ('type2',    '\"data2\"',    '\"meta2\"'),
    ('type3',    '\"data3\"',    '\"meta3\"')
 
-- Actual SQL statement of interest
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream
    DECLARE @count AS INT;
    DECLARE @streamIdInternal AS INT;
    BEGIN
        INSERT INTO dbo.Streams (StreamId, StreamIdOriginal) VALUES (@streamId, @streamId);
        SELECT @streamIdInternal = SCOPE_IDENTITY();

        INSERT INTO dbo.Events (StreamIdInternal, SequenceNumber, EventId, Created, [Type], JsonData, JsonMetadata)
            SELECT  @streamIdInternal,
                    SequenceNumber,
                    EventId,
                    Created,
                    [Type],
                    JsonData,
                    JsonMetadata
                FROM #Events
 
    END
    SELECT @streamIdInternal
COMMIT TRANSACTION CreateStream
 
DROP TABLE #Events