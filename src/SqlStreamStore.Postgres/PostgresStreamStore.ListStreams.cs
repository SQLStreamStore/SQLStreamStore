namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
    {
        protected override async Task<ListStreamsPage> ListStreamsInternal(
            string startsWith,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            Ensure.That(startsWith).IsNotNull();
            Ensure.That(maxCount).IsGt(0);
            if(!int.TryParse(continuationToken, out var afterIdInternal))
            {
                afterIdInternal = -1;
            }

            var streamIds = new List<string>();

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = BuildFunctionCommand(
                _schema.ListStreamsStartingWith,
                transaction,
                Parameters.Pattern(startsWith),
                Parameters.MaxCount(maxCount),
                Parameters.OptionalAfterIdInternal(afterIdInternal)))
            using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
            {
                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    streamIds.Add(reader.GetString(0));
                    afterIdInternal = reader.GetInt32(1);
                }
            }
            
            return new ListStreamsPage(afterIdInternal.ToString(), streamIds.ToArray(), listNextStreamsPage);
        }
    }
}