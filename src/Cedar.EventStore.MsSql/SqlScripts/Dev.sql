DROP TABLE dbo.Events
DROP TABLE dbo.Streams
 
CREATE TABLE dbo.Streams(
	StreamId CHAR(40) NOT NULL,
	StreamIdOriginal NVARCHAR(1000) NOT NULL,
	StreamIdInternal INT IDENTITY(1,1) NOT NULL,
	IsDeleted BIT NOT NULL DEFAULT ((0)),
	CONSTRAINT PK_Streams PRIMARY KEY CLUSTERED (StreamIdInternal)
);
 
CREATE UNIQUE NONCLUSTERED INDEX IX_Streams_StreamId ON dbo.Streams (StreamId);
 
CREATE TABLE dbo.Events(
	StreamIdInternal INT NOT NULL,
	[Checkpoint] int IDENTITY(1,1) NOT NULL,
	EventId UNIQUEIDENTIFIER NOT NULL,
	SequenceNumber INT NOT NULL,
	Created DATETIME NOT NULL,
	[Type] NVARCHAR(128) NOT NULL,
	JsonData NVARCHAR(max) NOT NULL,
	JsonMetadata NVARCHAR(max),
	CONSTRAINT PK_Events PRIMARY KEY CLUSTERED ([Checkpoint]),
	CONSTRAINT FK_Events_Streams FOREIGN KEY (StreamIdInternal) REFERENCES dbo.Streams(StreamIdInternal)
);
 
CREATE UNIQUE NONCLUSTERED INDEX [IX_Events_StreamIdInternal_SequenceNumber] ON [dbo].[Events] ([StreamIdInternal], [SequenceNumber]);
 
-- ExpectedVersion.NoStream
-- Will be inserting 1 - N events(row) in a transactions. Just using 3 here for demo.
 
DECLARE @sequenceNumber INT = 0;
DECLARE @streamId CHAR(40) = 'stream-1';
 
CREATE TABLE #Events (
    EventId         UNIQUEIDENTIFIER    default(NEWID())    NULL        ,
    SequenceNumber  int identity(0,1)                       NOT NULL    ,
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
	--SELECT @count = COUNT(*) FROM [dbo].[Streams] WHERE [StreamId]=@streamId;
	--IF @count = 0
	BEGIN
		-- Could generate this at runtime ; but the paramaterization feels icky
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
 
SELECT * FROM dbo.Streams
SELECT * FROM dbo.Events