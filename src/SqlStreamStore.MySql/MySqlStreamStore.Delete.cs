namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using MySql.Data.MySqlClient;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Infrastructure;
    using static Streams.Deleted;

    public partial class MySqlStreamStore
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

        protected override async Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var transaction = await connection.BeginTransactionAsync(cancellationToken).NotOnCapturedContext())
                {
                    var sqlStreamId = new StreamIdInfo(streamId).SqlStreamId;

                    if(await TryDeleteStreamMessage(connection, transaction, sqlStreamId, eventId, cancellationToken))
                    {
                        var eventDeletedEvent = CreateMessageDeletedMessage(sqlStreamId.IdOriginal, eventId);
                        await AppendToStreamExpectedVersionAny(
                            connection,
                            transaction,
                            MySqlStreamId.Deleted,
                            new[] { eventDeletedEvent },
                            cancellationToken);
                    }

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
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

                using(var transaction =
                    await connection.BeginTransactionAsync(cancellationToken).NotOnCapturedContext())
                {
                    var streamIdInternal = await GetStreamIdInternal(
                        streamIdInfo.SqlStreamId,
                        connection,
                        transaction,
                        cancellationToken);

                    if(streamIdInternal == default(int?))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                                streamIdInfo.SqlStreamId.IdOriginal,
                                expectedVersion));
                    }

                    var latestStreamVersion = await GetLatestStreamVersion(
                        streamIdInternal.Value,
                        connection,
                        transaction,
                        cancellationToken);

                    if(latestStreamVersion != expectedVersion)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                                streamIdInfo.SqlStreamId.IdOriginal,
                                expectedVersion));
                    }

                    await DeleteStreamMessagesWtfly(connection, transaction, streamIdInternal.Value, cancellationToken);

                    await DeleteStreamWtfly(connection, transaction, streamIdInternal, cancellationToken);

                    var streamDeletedEvent = CreateStreamDeletedMessage(streamIdInfo.SqlStreamId.IdOriginal);
                    var result = await AppendToStreamExpectedVersionAny(
                        connection,
                        transaction,
                        MySqlStreamId.Deleted,
                        new[] { streamDeletedEvent },
                        cancellationToken);

                    // Delete metadata stream (if it exists)
                    await DeleteStreamAnyVersion(connection,
                        transaction,
                        streamIdInfo.MetadataSqlStreamId,
                        cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using (var transaction = await connection.BeginTransactionAsync(cancellationToken).NotOnCapturedContext())
                {
                    await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.SqlStreamId, cancellationToken);

                    // Delete metadata stream (if it exists)
                    await DeleteStreamAnyVersion(connection, transaction, streamIdInfo.MetadataSqlStreamId, cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        private async Task DeleteStreamAnyVersion(
           MySqlConnection connection,
           MySqlTransaction transaction,
           MySqlStreamId streamId,
           CancellationToken cancellationToken)
        {
            if(await TryDeleteStreamAnyVersion(connection, transaction, streamId, cancellationToken))
            {
                var streamDeletedEvent = CreateStreamDeletedMessage(streamId.IdOriginal);
                
                await AppendToStreamExpectedVersionAny(
                    connection,
                    transaction,
                    MySqlStreamId.Deleted,
                    new[] { streamDeletedEvent },
                    cancellationToken);
            }
        }
        
        private async Task<bool> TryDeleteStreamMessage(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId sqlStreamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.DeleteStreamMessage, connection, transaction))
            {
                command.Parameters.AddWithValue("streamId", sqlStreamId.Id);
                command.Parameters.AddWithValue("eventId", eventId);
                var count = await command
                    .ExecuteScalarAsync(cancellationToken)
                    .NotOnCapturedContext();

                return (long) count > 1;
            }
        }

        private async Task<bool> TryDeleteStreamAnyVersion(
            MySqlConnection connection,
            MySqlTransaction transaction,
            MySqlStreamId streamId,
            CancellationToken cancellationToken)
        {
            var streamIdInternal = await GetStreamIdInternal(
                streamId,
                connection,
                transaction,
                cancellationToken);

            if(streamIdInternal == default(int?))
            {
                return false;
            }
 
            await DeleteStreamMessagesWtfly(connection, transaction, streamIdInternal.Value, cancellationToken);
            
            return await DeleteStreamWtfly(connection, transaction, streamIdInternal, cancellationToken);
        }

        private async Task<bool> DeleteStreamWtfly(
            MySqlConnection connection,
            MySqlTransaction transaction,
            int? streamIdInternal,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.WTF_DeleteStream, connection, transaction)
            {
                Parameters = { { "streamIdInternal", streamIdInternal } }
            })
            {
                return (await command.ExecuteNonQueryAsync(cancellationToken) > 0);
            }
        }

        private async Task DeleteStreamMessagesWtfly(
            MySqlConnection connection,
            MySqlTransaction transaction,
            int streamIdInternal,
            CancellationToken cancellationToken)
        {
            using(var command = new MySqlCommand(_scripts.WTF_DeleteStreamMessages, connection, transaction)
            {
                Parameters = { { "streamIdInternal", streamIdInternal } }
            })
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }
}
