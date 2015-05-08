namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.SqlScripts;
    using EnsureThat;

    public class MsSqlEventStore : IEventStore
    {
        private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();
        private readonly SqlConnection _connection;

        public MsSqlEventStore(Func<SqlConnection> createConnection)
        {
            Ensure.That(createConnection, "createConnection").IsNotNull();

            _connection = createConnection();
        }

        public Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
        {
            throw new System.NotImplementedException();
        }

        public Task DeleteStream(string streamId, int expectedVersion = ExpectedVersion.Any)
        {
            throw new System.NotImplementedException();
        }

        public Task<AllEventsPage> ReadAll(string checkpoint, int maxCount, ReadDirection direction = ReadDirection.Forward)
        {
            throw new System.NotImplementedException();
        }

        public Task<StreamEventsPage> ReadStream(string streamId, int start, int count, ReadDirection direction = ReadDirection.Forward)
        {
            throw new System.NotImplementedException();
        }

        public async Task InitializeStore(bool ignoreErrors = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            var cmd = new SqlCommand(Scripts.InitializeStore, _connection);
            if(ignoreErrors)
            {
                await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken));
            }
            else
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }

        public async Task DropAll(bool ignoreErrors = false, CancellationToken cancellationToken = default(CancellationToken))
        {
            var cmd = new SqlCommand(Scripts.DropAll, _connection);
            if (ignoreErrors)
            {
                await ExecuteAndIgnoreErrors(() => cmd.ExecuteNonQueryAsync(cancellationToken));
            }
            else
            {
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }


        private static async Task<T> ExecuteAndIgnoreErrors<T>(Func<Task<T>> operation)
        {
            try
            {
                return await operation();
            }
            catch
            {
                return default(T);
            }
        }

        public void Dispose()
        {
            if (_isDisposed.EnsureCalledOnce())
            {
                return;
            }
            _connection.Dispose();
        }
    }
}