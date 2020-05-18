IF OBJECT_ID('dbo.Messages', 'U') IS NOT NULL
BEGIN
    DROP TABLE [dbo].[Messages];
END

IF OBJECT_ID('dbo.Streams', 'U') IS NOT NULL
BEGIN
    DROP TABLE [dbo].[Streams];
END

IF OBJECT_ID('dbo.NewStreamMessages', 'U') IS NOT NULL
BEGIN
    DROP TYPE [dbo].[NewStreamMessages];
END