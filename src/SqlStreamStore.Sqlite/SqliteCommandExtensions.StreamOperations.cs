namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Data.Sqlite;

    public class StreamOperations
    {
        private readonly SqliteConnection _connection;
        private readonly string _streamId;

        private SqliteTransaction _transaction;

        public StreamOperations(SqliteConnection connection, string streamId)
        {
            _connection = connection;
            _streamId = streamId;
        }

        public IDisposable WithTransaction()
        {
            _transaction = _connection.BeginTransaction();
            return _transaction;
        }

        public Task Commit(CancellationToken cancellationToken = default)
        {
            _transaction.Commit();
            _transaction = null;
            return Task.CompletedTask;
        }

        public Task RollBack(CancellationToken cancellationToken = default)
        {
            _transaction.Rollback();
            _transaction = null;
            return Task.CompletedTask;
        }

        public Task<int> Length(CancellationToken cancellationToken)
        {
            using(var command = CreateCommand())
            {
                command.CommandText = @"SELECT COUNT(*)
                                        FROM messages
                                        JOIN streams on messages.stream_id_internal = streams.id_internal
                                        WHERE streams.id_original = @idOriginal";
                command.Parameters.AddWithValue("@idOriginal", _streamId);

                return Task.FromResult(command.ExecuteScalar(0));
            }
        }

        private SqliteCommand CreateCommand()
        {
            var cmd = _connection.CreateCommand();
            cmd.Transaction = _transaction;
            return cmd;
        }
    }
}