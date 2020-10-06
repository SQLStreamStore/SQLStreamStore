namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
    {
        protected override async Task<ListStreamsPage> ListStreamsInternal(
            Pattern pattern,
            int maxCount,
            string continuationToken,
            ListNextStreamsPage listNextStreamsPage,
            CancellationToken cancellationToken)
        {
            if(!int.TryParse(continuationToken, out var afterIdInternal))
            {
                afterIdInternal = -1;
            }

            var streamIds = new List<string>();

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = GetListStreamsCommand(pattern, maxCount, afterIdInternal, transaction))
            using(var reader = await command
                .ExecuteReaderAsync(cancellationToken)
                .ConfigureAwait(false))
            {
                while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    streamIds.Add(reader.GetString(0));
                    afterIdInternal = reader.GetInt32(1);
                }
            }

            return new ListStreamsPage(afterIdInternal.ToString(), streamIds.ToArray(), listNextStreamsPage);
        }

        private NpgsqlCommand GetListStreamsCommand(
            Pattern pattern,
            int maxCount,
            int? afterIdInternal,
            NpgsqlTransaction transaction)
        {
            switch(pattern)
            {
                case Pattern.Any _:
                    return BuildFunctionCommand(
                        _schema.ListStreams,
                        transaction,
                        Parameters.MaxCount(maxCount),
                        Parameters.OptionalAfterIdInternal(afterIdInternal));
                case Pattern.StartingWith p:
                    return BuildFunctionCommand(
                        _schema.ListStreamsStartingWith,
                        transaction,
                        Parameters.Pattern(pattern.Value),
                        Parameters.MaxCount(maxCount),
                        Parameters.OptionalAfterIdInternal(afterIdInternal));
                case Pattern.EndingWith p:
                    return BuildFunctionCommand(
                        _schema.ListStreamsEndingWith,
                        transaction,
                        Parameters.Pattern(pattern.Value),
                        Parameters.MaxCount(maxCount),
                        Parameters.OptionalAfterIdInternal(afterIdInternal));

                default:
                    throw Pattern.Unrecognized(nameof(pattern));
            }
        }
    }
}
