namespace SqlStreamStore
{
    using System;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using NpgsqlTypes;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    public partial class PostgresStreamStore
    {
        protected override async Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = _createConnection())
            {
                await connection.OpenAsync(cancellationToken).NotOnCapturedContext();

                using(var transaction = connection.BeginTransaction())
                {
                    if (await DeleteStreamInternal(
                        streamIdInfo.PostgresqlStreamId,
                        expectedVersion,
                        transaction,
                        cancellationToken)) {

                        var streamDeletedEvent = Deleted.CreateStreamDeletedMessage(streamId);

                        await AppendToStreamInternal(
                            PostgresqlStreamId.Deleted,
                            ExpectedVersion.Any,
                            new[] { streamDeletedEvent },
                            transaction,
                            cancellationToken);
                    }

                    await DeleteStreamInternal(
                        streamIdInfo.MetadataPosgresqlStreamId,
                        expectedVersion,
                        transaction,
                        cancellationToken);

                    await transaction.CommitAsync(cancellationToken).NotOnCapturedContext();
                }
            }
        }

        private async Task<bool> DeleteStreamInternal(
            PostgresqlStreamId postgresqlStreamId,
            int expectedVersion,
            NpgsqlTransaction transaction,
            CancellationToken cancellationToken)
        {
            using(var command = new NpgsqlCommand(_schema.DeleteStream, transaction.Connection, transaction)
            {
                CommandType = CommandType.StoredProcedure,
                Parameters =
                {
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Char,
                        Size = 42,
                        NpgsqlValue = postgresqlStreamId.Id
                    },
                    new NpgsqlParameter
                    {
                        NpgsqlDbType = NpgsqlDbType.Integer,
                        NpgsqlValue = expectedVersion
                    }
                }
            })
            {
                try
                {
                    var result = await command.ExecuteScalarAsync(cancellationToken).NotOnCapturedContext(); 
                    
                    return (int)result > 0;
                }
                catch(NpgsqlException ex)
                {
                    await transaction.RollbackAsync(cancellationToken).NotOnCapturedContext();

                    if(ex.Message.Contains("WrongExpectedVersion"))
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(
                                postgresqlStreamId.IdOriginal,
                                expectedVersion),
                            ex);
                    }

                    throw;
                }
            }
        }

        protected override Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}