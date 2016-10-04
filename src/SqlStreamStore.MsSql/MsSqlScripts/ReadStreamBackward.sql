/* SQL Server 2008+ */

    DECLARE @streamIdInternal AS INT
    DECLARE @lastStreamVersion AS INT

     SELECT @streamIdInternal = dbo.Streams.IdInternal, @lastStreamVersion = dbo.Streams.[Version]
       FROM dbo.Streams
      WHERE dbo.Streams.Id = @streamId

     SELECT TOP(@count)
            dbo.Messages.StreamVersion,
            dbo.Messages.Position,
            dbo.Messages.Id AS EventId,
            dbo.Messages.Created,
            dbo.Messages.Type,
            dbo.Messages.JsonData,
            dbo.Messages.JsonMetadata
       FROM dbo.Messages
 INNER JOIN dbo.Streams
         ON dbo.Messages.StreamIdInternal = dbo.Streams.IdInternal
      WHERE dbo.Messages.StreamIDInternal = @streamIDInternal AND dbo.Messages.StreamVersion <= @StreamVersion
   ORDER BY dbo.Messages.Position DESC

     SELECT @lastStreamVersion
