namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;

    public class SqlWatcher : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly string _connectionString;
        private readonly IEventStore _eventStore;
        private readonly InterlockedBoolean _fetchingEvents = new InterlockedBoolean();
        private SqlCommand _command;
        private string _checkpoint;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        public SqlWatcher(string connectionString, IEventStore eventStore)
        {
            _connectionString = connectionString;
            _eventStore = eventStore;
            _connection = new SqlConnection(_connectionString);
            SqlDependency.Start(connectionString);
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _command.Dispose();
            SqlConnection.ClearPool(_connection);
            _connection.Dispose();
            SqlDependency.Stop(_connectionString);
        }

        public async Task Start()
        {
            var allEventsPage = await _eventStore.ReadAll(_eventStore.EndCheckpoint, 1, ReadDirection.Backward);
            _checkpoint = allEventsPage.NextCheckpoint;

            await _connection.OpenAsync();

            Subscribe();

            FetchEvents();
        }

        private void Subscribe()
        {
            _command = new SqlCommand("SELECT Id FROM dbo.Events", _connection)
            {
                Notification = null
            };
            var sqlDependency = new SqlDependency(_command);
            sqlDependency.OnChange += SqlDependencyOnOnChange;

            _command.ExecuteNonQuery();
        }

        private void SqlDependencyOnOnChange(object sender, SqlNotificationEventArgs sqlNotificationEventArgs)
        {
            var dependency = (SqlDependency) sender;
            dependency.OnChange -= SqlDependencyOnOnChange;

            Subscribe();

            FetchEvents();
        }


        /// <summary>
        /// Fetches any newly saved events and reises them to subscribers accordingly.
        /// This is method is designed to run a *single* background process at any given time.
        /// </summary>
        private void FetchEvents()
        {
            if(_fetchingEvents.EnsureCalledOnce())
            {
                return;
            }
            Task.Run(async () =>
            {
                bool isEnd = false;
                while(!isEnd)
                {
                    if(_cancellationTokenSource.IsCancellationRequested)
                    {
                        return;
                    }

                    var allEventsPage = await _eventStore
                        .ReadAll(_checkpoint, 10, cancellationToken: _cancellationTokenSource.Token)
                        .NotOnCapturedContext();

                    isEnd = allEventsPage.IsEnd;

                    _checkpoint = allEventsPage.NextCheckpoint;

                    Console.WriteLine(allEventsPage.NextCheckpoint);
                }

                _fetchingEvents.Set(false);
            }, _cancellationTokenSource.Token);
        }
    }
}