namespace SqlStreamStore.Oracle
{
    using System.Threading;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using global::Oracle.ManagedDataAccess.Types;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class OracleStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                var result = await AppendToStreamInternal(transaction,
                    streamIdInfo,
                    expectedVersion,
                    messages,
                    cancellationToken);
                
                transaction.Commit();
                
                return result;
            }
        }
        
        private async Task<AppendResult> AppendToStreamInternal(
            OracleTransaction transaction,
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            
            var command = _commandBuilder.Append(
                transaction,
                streamIdInfo,
                expectedVersion,
                messages
            );

            return await AppendExecute(streamIdInfo, expectedVersion, command, cancellationToken);
        }

        private async Task<AppendResult> AppendExecute(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            OracleCommand command,
            CancellationToken cancellationToken
        )
        {
            using(command)
            {
                try
                {
                    using(await command
                        .ExecuteReaderAsync(cancellationToken)
                        .NotOnCapturedContext())
                    {
                        var currentVersion =
                            ((OracleDecimal) command.Parameters["oVersion"]
                                .Value).ToInt32();
                        var currentPosition =
                            ((OracleDecimal) command.Parameters["oPosition"]
                                .Value).ToInt64();

                        return new AppendResult(currentVersion, currentPosition);
                    }
                }
                catch(OracleException ex)
                {
                    if(ex.Number == 20001 || ex.Number == 20002)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.StreamId.IdOriginal, expectedVersion),
                            streamIdInfo.StreamId.IdOriginal,
                            ExpectedVersion.Any,
                            ex);
                    }

                    throw;
                }
            }
        }
    }
}