namespace Cedar.EventStore.Subscriptions
{
    using System;
    using System.Collections.Concurrent;
    using System.Data.SqlClient;
    using System.Threading;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
    using Cedar.EventStore.Streams;
    using Timer = System.Timers.Timer;

    public class SqlEventsWatcher : IDisposable
    {
        private readonly SqlConnection _connection;
        private readonly string _connectionString;
        private readonly IEventStore _eventStore;
        private readonly bool _callSqlDependencyStartStop;
        private readonly InterlockedBoolean _fetchingEvents = new InterlockedBoolean();
        private SqlCommand _command;
        private string _checkpoint;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly Timer _timer;
        private readonly ConcurrentDictionary<Guid, IDisposable> _subscriptions = new ConcurrentDictionary<Guid, IDisposable>();

        /// <summary>
        ///     Initialized a new instance of <see cref="SqlEventsWatcher" />
        /// </summary>
        /// <param name="connectionString">
        ///     The connection string to the event store. Used for monitoring commits with SqlDependency.
        /// </param>
        /// <param name="eventStore">
        ///     An IEventStore instance.
        /// </param>
        /// <param name="pollingInterval">
        ///     SqlDependency is not 100% reliable. Occasionally polling will catch situations where a notification
        ///     can be missed. See https://connect.microsoft.com/SQLServer/feedback/details/543921/sqldependency-incorrect-behaviour-after-sql-server-restarts
        /// </param>
        /// <param name="callSqlDependencyStartStop">
        ///     <see cref="SqlDependency"/>.Start is a nasty global static thing. As this is a class library the host
        ///     application may be doing it's own SqlDependency work with this query string. In that case, the host
        ///     application owns that operation and should call it accordingly. In such cases this paramater should be
        ///     be set to false. Defaults is true. See http://stackoverflow.com/a/9120471 
        /// </param>
        public SqlEventsWatcher(
            string connectionString,
            IEventStore eventStore,
            int pollingInterval = 5000,
            bool callSqlDependencyStartStop = true)
        {
            _connectionString = connectionString;
            _eventStore = eventStore;
            _callSqlDependencyStartStop = callSqlDependencyStartStop;
            _connection = new SqlConnection(_connectionString);

            if(callSqlDependencyStartStop)
            {
                SqlDependency.Start(connectionString);
            }

            _timer = new System.Timers.Timer(pollingInterval)
            {
                AutoReset = true
            };

            _timer.Elapsed += (_, __) =>
            {
                FetchEvents();
            };
            _timer.Start();
        }

        public void Dispose()
        {
            _cancellationTokenSource.Cancel();
            _timer.Dispose();
            _command.Dispose();
            SqlConnection.ClearPool(_connection);
            _connection.Dispose();

            if(_callSqlDependencyStartStop)
            {
                SqlDependency.Stop(_connectionString);
            }
        }

        public async Task Start()
        {
            var allEventsPage = await _eventStore.ReadAll(_eventStore.EndCheckpoint, 1, ReadDirection.Backward);
            _checkpoint = allEventsPage.NextCheckpoint;
            await _connection.OpenAsync();

            SetupSqlDependency();

            FetchEvents();
        }

        public IStreamSubscription SubscribeToStream(string streamId, EventReceived eventReceived, SubscriptionDropped subscriptionDropped)
        {
            Guid subscriptionId = Guid.NewGuid();
            var streamSubscription = new StreamSubscription(streamId, eventReceived, subscriptionDropped,
                () =>
                {
                    IDisposable _;
                    _subscriptions.TryRemove(subscriptionId, out _);
                });
            _subscriptions.TryAdd(subscriptionId, streamSubscription);
            return Subscriptions;
        }

        private void SetupSqlDependency()
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

            SetupSqlDependency();

            FetchEvents();
        }

        /// <summary>
        /// Fetches any newly saved events and raises them to subscribers accordingly.
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
                // TODO exception / error handling
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

                    //TODO Push events from allEventsPage to subscribers.
                }

                _fetchingEvents.Set(false);
            }, _cancellationTokenSource.Token);
        }

        private class StreamSubscription : IStreamSubscription
        {
            private EventReceived _eventReceived;
            private SubscriptionDropped _subscriptionDropped;
            private readonly Action _onDispose;

            public StreamSubscription(
                string streamId,
                EventReceived eventReceived,
                SubscriptionDropped subscriptionDropped,
                Action onDispose)
            {
                StreamId = streamId;
                _eventReceived = streamEvent =>
                {
                    LastVersion = streamEvent.StreamVersion;
                    return eventReceived(streamEvent);
                };
                _subscriptionDropped = subscriptionDropped;
                _onDispose = onDispose;
            }

            public void Dispose()
            {
                _onDispose();
            }

            public string StreamId { get; }

            public int LastVersion { get; private set; }
        }
    }
}