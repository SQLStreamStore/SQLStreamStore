SELECT TOP (@MaxCount) dbo.Streams.IdOriginal, dbo.Streams.IdInternal
FROM dbo.Streams
WHERE dbo.Streams.IdInternal > @AfterIdInternal
AND dbo.Streams.IdOriginal LIKE CONCAT(@Pattern, '%')
ORDER BY dbo.Streams.IdInternal ASC