CREATE TABLE [dbo].[Streams](
	[StreamId] [char](40) NOT NULL,
	[StreamIdOriginal] [nvarchar](1000) NOT NULL,
	[StreamIdInternal] [int] IDENTITY(1,1) NOT NULL,
	[IsDeleted] [bit] NOT NULL DEFAULT ((0)),
	CONSTRAINT [PK_Streams] PRIMARY KEY CLUSTERED ([StreamIdInternal])
);

CREATE UNIQUE NONCLUSTERED INDEX [IX_Streams_StreamId] ON [dbo].[Streams] (StreamId);

CREATE TABLE [dbo].[Events](
	[StreamIdInternal] [int] NOT NULL,
	[Checkpoint] [int] IDENTITY(1,1) NOT NULL,
	[SequenceNumber] [int] NOT NULL,
	[Created] [datetime] NOT NULL,
	[Type] [nvarchar](128) NOT NULL,
	[JsonData] [nvarchar](max) NOT NULL,
	[JsonMetadata] [nvarchar](max),
	CONSTRAINT [PK_Events] PRIMARY KEY CLUSTERED ([Checkpoint]),
	CONSTRAINT [FK_Events_Streams] FOREIGN KEY (StreamIdInternal) REFERENCES [dbo].[Streams] ([StreamIdInternal])
);

CREATE UNIQUE NONCLUSTERED INDEX [IX_Events_StreamIdInternal_SequenceNumber] ON [dbo].[Events] ([StreamIdInternal], [SequenceNumber]);