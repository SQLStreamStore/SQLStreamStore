/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT
    DECLARE @lastStreamPosition AS BIGINT
    DECLARE @maxAge AS INT
    DECLARE @maxCount AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal,
            @lastStreamVersion = dbo.Streams.[Version],
            @lastStreamPosition = dbo.Streams.[Position],
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT @lastStreamVersion, @lastStreamPosition

     SELECT TOP(@count)
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id AS EventId,
            dbo.Messages.Created,
            dbo.Messages.Type,
            dbo.Messages.JsonMetadata,
            @maxAge
       FROM dbo.Messages
 INNER JOIN dbo.Streams
         ON dbo.Messages.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Messages.StreamIdInternal = @streamIdInternal AND dbo.Messages.StreamVersion <= @streamVersion
   ORDER BY dbo.Messages.Position DESC
