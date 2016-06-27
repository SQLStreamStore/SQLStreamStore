namespace Cedar.EventStore
{
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public partial class MsSqlEventStore
    {
        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            return expectedVersion == ExpectedVersion.Any
                ? DeleteStreamAnyVersion(streamIdInfo, cancellationToken)
                : DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
        }

        protected override Task DeleteEventInternal(string streamId, int streamVersion, CancellationToken cancellationToken)
        {
            throw new System.NotImplementedException();
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken);

                using (var transaction = connection.BeginTransaction())
                {
                    using (var command = new SqlCommand(_scripts.DeleteStreamAnyVersion, connection, transaction))
                    {
                        command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                        await command
                            .ExecuteNonQueryAsync(cancellationToken)
                            .NotOnCapturedContext();
                    }

                    var streamDeletedEvent = Deleted.CreateStreamDeletedEvent(streamIdInfo.Id);
                    await AppendToStreamExpectedVersionAny(
                        connection,
                        transaction,
                        new StreamIdInfo(Deleted.StreamId),
                        new[] { streamDeletedEvent },
                        cancellationToken);

                    transaction.Commit();
                }
            }
        }

        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                {
                    using(var command = new SqlCommand(_scripts.DeleteStreamExpectedVersion, connection, transaction))
                    {
                        command.Parameters.AddWithValue("streamId", streamIdInfo.Hash);
                        command.Parameters.AddWithValue("expectedStreamVersion", expectedVersion);
                        try
                        {
                            await command
                                .ExecuteNonQueryAsync(cancellationToken)
                                .NotOnCapturedContext();
                        }
                        catch(SqlException ex)
                        {
                            transaction.Rollback();
                            if(ex.Message.StartsWith("WrongExpectedVersion"))
                            {
                                throw new WrongExpectedVersionException(
                                    Messages.DeleteStreamFailedWrongExpectedVersion(streamIdInfo.Id, expectedVersion),
                                    ex);
                            }
                            throw;
                        }

                        var streamDeletedEvent = Deleted.CreateStreamDeletedEvent(streamIdInfo.Id);
                        await AppendToStreamExpectedVersionAny(
                            connection,
                            transaction,
                            new StreamIdInfo(Deleted.StreamId),
                            new[] { streamDeletedEvent },
                            cancellationToken);

                        transaction.Commit();
                    }
                }
            }
        }
    }
}
