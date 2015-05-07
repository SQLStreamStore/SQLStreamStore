BEGIN TRANSACTION
GO
IF EXISTS(SELECT * FROM sysobjects WHERE name='Streams' AND xtype = 'U') RETURN;
CREATE TABLE dbo.Streams
(
    StoreId varchar(40) NOT NULL,
	StreamId char(40) NOT NULL,
	StreamIdOriginal nvarchar(1000) NOT NULL,
	StreamInternalId int NOT NULL IDENTITY (1, 1)
);
GO
COMMIT