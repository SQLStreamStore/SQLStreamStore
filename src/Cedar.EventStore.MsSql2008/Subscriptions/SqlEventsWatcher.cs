namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Cedar.EventStore.Streams;

    public class SqlEventsWatcher : IDisposable
    {
        private readonly string _connectionString;
        private SqlConnection _connection;
        private SqlCommand _command;
        private SqlDependency _sqlDependency;

        public SqlEventsWatcher(string connectionString)
        {
            _connectionString = connectionString;
            bool b = SqlDependency.Start(connectionString);
        }

        public async Task Initialize()
        {
            _connection = new SqlConnection(_connectionString);
            await _connection.OpenAsync();

            _command = new SqlCommand("SELECT Id, Ordinal FROM dbo.Events", _connection);
            _command.Notification = null;

            _sqlDependency = new SqlDependency(_command);
            _sqlDependency.OnChange += (sender, args) =>
            {
                Console.WriteLine("here 1");
            };

            await _command.ExecuteNonQueryAsync();
        }

        public void Dispose()
        {
            _command.Dispose();
            _connection.Dispose();
            SqlDependency.Stop(_connectionString);
        }

        public IStreamSubscription SubscribeToStream(string streamId, EventReceived eventReceived, SubscriptionDropped subscriptionDropped)
        {
            return new StreamSubscription(streamId, eventReceived, subscriptionDropped);
        }

        private class StreamSubscription : IStreamSubscription
        {
            private EventReceived _eventReceived;
            private SubscriptionDropped _subscriptionDropped;

            public StreamSubscription(string streamId, EventReceived eventReceived, SubscriptionDropped subscriptionDropped)
            {
                StreamId = streamId;
                _eventReceived = eventReceived;
                _subscriptionDropped = subscriptionDropped;
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public string StreamId { get; }

            public int LastEventNumber
            {
                get { throw new NotImplementedException(); }
            }

            public Task Connect()
            {
                throw new NotImplementedException();
            }
        }
    }
}