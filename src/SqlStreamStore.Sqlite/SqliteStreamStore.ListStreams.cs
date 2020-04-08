namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            
            if(!int.TryParse(continuationToken, out var idInternal))
            {
                idInternal = -1;
            }

            using(var conn = OpenConnection())
            using(var cmd = conn.CreateCommand())
            {
                var streamIds = new List<string>();
                cmd.CommandText = @"SELECT streams.id_original, streams.id_internal
FROM streams
WHERE streams.id_internal > @idInternal";
                switch(pattern)
                {
                    case Pattern.StartingWith _:
                        cmd.CommandText += "\n AND streams.id_original LIKE CONCAT(@Pattern), '%')";
                        break;
                    case Pattern.EndingWith _:
                        cmd.CommandText += "\n AND streams.id_original LIKE CONCAT('%', @Pattern)";
                        break;
                }

                cmd.CommandText += "\n LIMIT @maxCount;";
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@idInternal", idInternal);
                cmd.Parameters.AddWithValue("@pattern", pattern.Value);
                cmd.Parameters.AddWithValue("@maxCount", maxCount);

                using(var reader = cmd.ExecuteReader())
                {
                    while(reader.Read())
                    {
                        streamIds.Add(reader.GetString(0));
                        idInternal = reader.GetInt32(1);
                    }
                }

                return Task.FromResult(new ListStreamsPage(idInternal.ToString(), streamIds.ToArray(), listNextStreamsPage));
            }
        }
    }
}