namespace SqlStreamStore.V1
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.V1.Infrastructure;
    using SqlStreamStore.V1.MySqlScripts;
    using SqlStreamStore.V1.Streams;

    partial class MySqlStreamStore
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
                .NotOnCapturedContext())
            {
                while(await reader.ReadAsync(cancellationToken).NotOnCapturedContext())
                {
                    streamIds.Add(reader.GetString(0));
                    afterIdInternal = reader.GetInt32(1);
                }
            }

            return new ListStreamsPage(afterIdInternal.ToString(), streamIds.ToArray(), listNextStreamsPage);
        }

        private MySqlCommand GetListStreamsCommand(
            Pattern pattern,
            int maxCount,
            int? afterIdInternal,
            MySqlTransaction transaction)
        {
            switch(pattern)
            {
                case Pattern.Any _:
                    return BuildStoredProcedureCall(
                        _schema.ListStreams,
                        transaction,
                        Parameters.MaxCount(maxCount),
                        Parameters.OptionalAfterIdInternal(afterIdInternal));
                case Pattern.StartingWith p:
                    return BuildStoredProcedureCall(
                        _schema.ListStreamsStartingWith,
                        transaction,
                        Parameters.Pattern(pattern.Value),
                        Parameters.MaxCount(maxCount),
                        Parameters.OptionalAfterIdInternal(afterIdInternal));
                case Pattern.EndingWith p:
                    return BuildStoredProcedureCall(
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