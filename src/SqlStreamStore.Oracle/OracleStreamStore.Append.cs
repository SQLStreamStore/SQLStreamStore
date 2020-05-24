namespace SqlStreamStore.Oracle
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using global::Oracle.ManagedDataAccess.Types;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class OracleStreamStore
    {
        private class OracleAppendResult
        {
            public int CurrentVersion { get; }
            
            public long CurrentPosition { get; }

            public OracleAppendResult(
                int currentVersion,
                long currentPosition)
            {
                CurrentVersion = currentVersion;
                CurrentPosition = currentPosition;
            }
        }
        
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                var result = await AppendToStreamInternal(
                    transaction,
                    streamIdInfo,
                    expectedVersion,
                    messages,
                    cancellationToken);
                
                transaction.Commit();

                return result;
            }
        }

        

        private Task<AppendResult> AppendToStreamInternal(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken
        )
        {
            GuardAgainstDisposed();
            
            if(expectedVersion == ExpectedVersion.Any)
            {
                return AppendToStreamAnyVersion(
                    transaction,
                    streamInfo,
                    messages,
                    cancellationToken);
            }
            if(expectedVersion == ExpectedVersion.NoStream)
            {
                return AppendToStreamNoStream(
                    transaction,
                    streamInfo,
                    messages,
                    cancellationToken);
            }
            if(expectedVersion == ExpectedVersion.EmptyStream)
            {
                return AppendToStreamExpectedVersion(
                    transaction,
                    streamInfo,
                    -1,
                    messages,
                    cancellationToken);
            }
            
            return  AppendToStreamExpectedVersion(
                transaction,
                streamInfo,
                expectedVersion,
                messages,
                cancellationToken);
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

                        if(!(command.Parameters["oDeletedEvents"].Value is DBNull))
                        {
                            await HandleDeletedEventsFeedback(command.Transaction, command, (OracleRefCursor) command.Parameters["oDeletedEvents"].Value, cancellationToken);
                        }
                        
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
        
        private Task<AppendResult> AppendToStreamAnyVersion(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var command = _commandBuilder.AppendToStreamAnyVersion(
                transaction,
                streamInfo,
                messages
            );

            return AppendExecute(streamInfo, ExpectedVersion.Any, command, cancellationToken);
        }
        
        private Task<AppendResult> AppendToStreamNoStream(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var command = _commandBuilder.AppendToStreamNoStream(
                transaction,
                streamInfo,
                messages
            );
            
            return AppendExecute(streamInfo, ExpectedVersion.NoStream, command, cancellationToken);
        }

        private Task<AppendResult> AppendToStreamExpectedVersion(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            var command = _commandBuilder.AppendToStreamExpectedVersion(
                transaction,
                streamInfo,
                expectedVersion,
                messages
            );
            
            return AppendExecute(streamInfo, expectedVersion, command, cancellationToken);
        }
    }
}