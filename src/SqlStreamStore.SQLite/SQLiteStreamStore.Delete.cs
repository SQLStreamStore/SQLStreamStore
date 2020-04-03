namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;
    using SqlStreamStore.Streams;

    public partial class SQLiteStreamStore
    {
        protected override Task DeleteStreamInternal(
            string streamId,
            int expectedVersion,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            
            using (var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            {
                DeleteStreamInternal(streamIdInfo.SQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                DeleteStreamInternal(streamIdInfo.MetadataSQLiteStreamId,
                    expectedVersion,
                    transaction,
                    cancellationToken);

                transaction.Commit();
            }
            
            return Task.CompletedTask;
        }


        private void DeleteStreamInternal(
            SQLiteStreamId streamId,
            int expectedVersion,
            SqliteTransaction transaction,
            CancellationToken cancellationToken)
        {
            int streamIdInternal = 0;
            using(var command = transaction.Connection.CreateCommand())
            {
                if(expectedVersion == ExpectedVersion.EmptyStream)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                        streamId.IdOriginal,
                        expectedVersion,
                        default);
                }
                else if(expectedVersion >= 0) // expected version
                {
                    command.CommandText = "SELECT id_internal FROM streams WHERE id = @streamId";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamId", streamId.Id);
                    var result = command.ExecuteScalar();
                    if(result == DBNull.Value)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion,
                            default);
                    }

                    streamIdInternal = Convert.ToInt32(result);

                    command.CommandText = @"SELECT messages.stream_version 
                                        FROM messages 
                                        WHERE messages.stream_id_internal = @streamIdInternal 
                                        ORDER BY messages.position DESC
                                        LIMIT 1;";
                    command.Parameters.Clear();
                    command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                    var latestVersion = Convert.ToInt32(command.ExecuteScalar());

                    if(expectedVersion != latestVersion)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.DeleteStreamFailedWrongExpectedVersion(streamId.IdOriginal, expectedVersion),
                            streamId.IdOriginal,
                            expectedVersion,
                            default);
                    }
                }

                command.CommandText = @"DELETE FROM messages WHERE messages.stream_id_internal = @streamIdInternal;
                                            DELETE FROM streams WHERE streams.id = @streamId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamIdInternal", streamIdInternal);
                command.Parameters.AddWithValue("@streamId", streamId.Id);
                command.ExecuteNonQuery();

                //TODO: develop deletion tracking, if required.
            }
        }

        protected override Task DeleteEventInternal(
            string streamId,
            Guid eventId,
            CancellationToken cancellationToken)
        {
            var streamIdInfo = new StreamIdInfo(streamId);
            using(var connection = OpenConnection())
            using(var transaction = connection.BeginTransaction())
            {
                DeleteEventInternal(streamIdInfo, eventId, transaction, cancellationToken);
                
                transaction.Commit();
            }

            return Task.CompletedTask;
        }

        private void DeleteEventInternal(
            StreamIdInfo streamIdInfo,
            Guid eventId,
            SqliteTransaction transaction,
            CancellationToken cancellationToken)
        {
            // Note: WE may need to put work in to append to stream expectedversionany.  reference DeleteStreamMessage.sql for more information.
            // var deletedMessageMessage = Deleted.CreateMessageDeletedMessage(
            //     streamIdInfo.SQLiteStreamId.IdOriginal,
            //     eventId);

            using(var command = transaction.Connection.CreateCommand())
            {
                command.CommandText = @"DELETE FROM messages 
                WHERE messages.stream_id_internal = (SELECT id_internal FROM streams WHERE streams.id = @streamId)
                    AND messages.message_id = @messageId";
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@streamId", streamIdInfo.SQLiteStreamId.Id);
                command.Parameters.AddWithValue("@messageId", eventId);

                command.ExecuteNonQuery();
            }
        }
    }
}