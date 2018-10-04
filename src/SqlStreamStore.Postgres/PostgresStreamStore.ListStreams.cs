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
        public override async Task<ListStreamsPage> ListStreams(
            string startsWith,
            int startingAt = 0,
            int maxCount = 100,
            CancellationToken cancellationToken = default)
        {
            Ensure.That(startsWith).IsNotNull();
            Ensure.That(startingAt).IsGte(0);
            Ensure.That(maxCount).IsGt(0);

            var streamIds = new List<string>();
            var afterIdInternal = 0;

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = BuildFunctionCommand(
                _schema.ListStreamsStartingWith,
                transaction,
                Parameters.Pattern(startsWith),
                Parameters.OptionalStartingAt(startingAt),
                Parameters.MaxCount(maxCount),
                Parameters.OptionalAfterIdInternal(default)))
            using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
            {
                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    streamIds.Add(reader.GetString(0));
                    afterIdInternal = reader.GetInt32(1);
                }
            }

            async Task<ListStreamsPage> Next(int s, CancellationToken ct)
            {
                var streamIds2 = new List<string>();
                using(var connection = await OpenConnection(cancellationToken))
                using(var transaction = connection.BeginTransaction())
                using(var command = BuildFunctionCommand(
                    _schema.ListStreamsStartingWith,
                    transaction,
                    Parameters.Pattern(startsWith),
                    Parameters.OptionalStartingAt(default),
                    Parameters.MaxCount(maxCount),
                    Parameters.OptionalAfterIdInternal(afterIdInternal)))
                using(var reader = await command.ExecuteReaderAsync(cancellationToken).NotOnCapturedContext())
                {
                    while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                    {
                        streamIds2.Add(reader.GetString(0));
                        afterIdInternal = reader.GetInt32(1);
                    }
                }

                return new ListStreamsPage(s + maxCount, streamIds2.ToArray(), Next);
            }

            return new ListStreamsPage(startingAt, streamIds.ToArray(), Next);
        }
    }
}