namespace SqlStreamStore.Oracle
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using global::Oracle.ManagedDataAccess.Types;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Streams;

    partial class OracleStreamStore
    {
        protected override async Task DeleteStreamInternal(string streamId, int expectedVersion, CancellationToken cancellationToken)
        {
            try
            {
                var streamIdInfo = new StreamIdInfo(streamId);
                if(expectedVersion == ExpectedVersion.Any)
                {
                     await DeleteStreamAnyVersion(streamIdInfo, cancellationToken);
                     return;
                }

                await DeleteStreamExpectedVersion(streamIdInfo, expectedVersion, cancellationToken);
            }
            catch(OracleException ex)
            {
                if(ex.Number == 20001 || ex.Number == 20002)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(streamId, ExpectedVersion.Any),
                        streamId,
                        ExpectedVersion.Any,
                        ex);
                }

                throw;
            }
        }
        
        private async Task DeleteStreamExpectedVersion(
            StreamIdInfo streamIdInfo,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            using (var trans = await StartTransaction(cancellationToken))
            using(var cmdDelete =
                _commandBuilder.DeleteStreamExpectedVersion(trans, streamIdInfo, expectedVersion))
            {
                await DeleteStreamExecute(streamIdInfo, trans, cmdDelete, cancellationToken);
                
                trans.Commit();
            }
        }
        
        private async Task DeleteStreamAnyVersion(
            StreamIdInfo streamIdInfo,
            CancellationToken cancellationToken)
        {
            using (var trans = await StartTransaction(cancellationToken))
            using(var cmdDelete = _commandBuilder.DeleteStreamAnyVersion(trans, streamIdInfo))
            {
                await DeleteStreamExecute(streamIdInfo, trans, cmdDelete, cancellationToken);
                
                trans.Commit();
            }
        }

        private async Task DeleteStreamExecute(StreamIdInfo streamIdInfo, OracleTransaction trans, OracleCommand command, CancellationToken cancellationToken)
        {
            await command
                .ExecuteNonQueryAsync(cancellationToken)
                .NotOnCapturedContext();

            if((OracleDecimal)command.Parameters["oDeletedStream"].Value > 0)
            {
                if((OracleDecimal) command.Parameters["oDeletedMetaStream"].Value > 0)
                {
                    await AppendStreamDeletedMessage(trans, new [] { streamIdInfo.StreamId, streamIdInfo.MetaStreamId }, cancellationToken);
                }
                else
                {
                    await AppendStreamDeletedMessage(trans, new [] { streamIdInfo.StreamId }, cancellationToken);
                }
            }
        }
        
        

        protected override async Task DeleteEventInternal(string streamId, Guid eventId, CancellationToken cancellationToken)
        {
            using(var trans = await StartTransaction(cancellationToken))
            using (var command = _commandBuilder.DeleteMessage(trans, new OracleStreamId(streamId), eventId))
            {
                var count = await command
                    .ExecuteNonQueryAsync(cancellationToken)
                    .NotOnCapturedContext();

                if(count == 1)
                {
                    await AppendDeletedMessages(trans, new[] { (streamId, eventId) }, cancellationToken);
                }
                
                trans.Commit();
            }
        }

        private async Task AppendDeletedMessages(OracleTransaction transaction, IEnumerable<(string, Guid)> deletedMessages, CancellationToken cancellationToken)
        {
            if(_settings.DisableDeletionTracking)
                return;
            
            if(deletedMessages == null || !deletedMessages.Any())
                return;
            
            await AppendToStreamInternal(
                transaction,
                StreamIdInfo.Deleted,
                ExpectedVersion.Any,
                deletedMessages.Select(deleted => Deleted.CreateMessageDeletedMessage(deleted.Item1, deleted.Item2)).ToArray(),
                cancellationToken);
        }
        
        private async Task AppendStreamDeletedMessage(OracleTransaction transaction, OracleStreamId[] streams, CancellationToken cancellationToken)
        {
            if(_settings.DisableDeletionTracking)
                return;
            
            if(streams == null || !streams.Any())
                return;
            
            await AppendToStreamInternal(
                transaction,
                StreamIdInfo.Deleted,
                ExpectedVersion.Any,
                streams.Select(deleted => Deleted.CreateStreamDeletedMessage(deleted.IdOriginal)).ToArray(),
                cancellationToken);
        }

    }
}