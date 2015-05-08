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
 
CREATE UNIQUE NONCLUSTERED INDEX IX_Events_StreamIdInternal_SequenceNumber ON dbo.Events (StreamIdInternal, SequenceNumber);