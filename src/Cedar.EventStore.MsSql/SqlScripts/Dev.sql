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
DECLARE @sequenceNumber INT = 0;
DECLARE @streamId CHAR(40) = 'stream-1';

DECLARE @eventId_1 UNIQUEIDENTIFIER = NEWID();
DECLARE @created_1 DATETIME = GETDATE();
DECLARE @type_1 NVARCHAR(128) = 'type1';
DECLARE @jsonData_1 NVARCHAR(max) = '\"data1\"';
DECLARE @jsonMetadata_1 NVARCHAR(max) = '\"meta1\"';

DECLARE @eventId_2 UNIQUEIDENTIFIER = NEWID();
DECLARE @created_2 DATETIME = GETDATE();
DECLARE @type_2 NVARCHAR(128) = 'type1';
DECLARE @jsonData_2 NVARCHAR(max) = '\"data2\"';
DECLARE @jsonMetadata_2 NVARCHAR(max) = '\"meta2\"';

SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRANSACTION CreateStream
	DECLARE @count AS INT;
	DECLARE @streamIdInternal AS INT;
	SELECT @count = COUNT(*) FROM [dbo].[Streams] WHERE [StreamId]=@streamId;
	PRINT @count;
	BEGIN
		INSERT INTO dbo.Streams (StreamId, StreamIdOriginal) VALUES (@streamId, @streamId);
		SELECT @streamIdInternal = SCOPE_IDENTITY();
		
		INSERT INTO dbo.Events (StreamIdInternal, SequenceNumber, EventId, Created, [Type], JsonData, JsonMetadata)
			VALUES (@streamIdInternal, @sequenceNumber, @eventId_1, @created_1, @type_1, @jsonData_1, @jsonMetadata_1);
		
		SET @sequenceNumber = @sequenceNumber + 1
		INSERT INTO dbo.Events (StreamIdInternal, SequenceNumber, EventId, Created, [Type], JsonData, JsonMetadata)
			VALUES (@streamIdInternal, @sequenceNumber, @eventId_2, @created_2, @type_2, @jsonData_2, @jsonMetadata_2)
		-- This can continue for N inserts of N events

    END
    SELECT @streamIdInternal
COMMIT TRANSACTION CreateStream

SELECT * FROM dbo.Streams
SELECT * FROM dbo.Events