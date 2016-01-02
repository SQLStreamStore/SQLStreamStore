namespace Cedar.EventStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class ExploratoryTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public ExploratoryTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

        [Fact]
        public async Task SqlDependencyExplorations()
        {
            using(var fixture = new MsSqlEventStoreFixture())
            {
                using(var eventStore = await fixture.GetEventStore())
                {
                    Func<int, int, Task> createStreams = async (count, interval) =>
                    {
                        for(int i = 0; i < count; i++)
                        {
                            var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                            await eventStore.AppendToStream($"stream-{i}", ExpectedVersion.NoStream, newStreamEvent);

                            await Task.Delay(interval);
                        }
                    };

                    var allEventsPage = await eventStore.ReadAll(Checkpoint.End, 1, ReadDirection.Backward);

                    var checkpoint = allEventsPage.NextCheckpoint;

                    var task = createStreams(0, 100);

                    using (var watcher = new SqlWatcher(fixture.ConnectionString, _testOutputHelper, eventStore, checkpoint))
                    {
                        await watcher.Start();

                        await task;
                    }
                }
            }
        }

        private class SqlWatcher : IDisposable
        {
            private readonly string _connectionString;
            private readonly ITestOutputHelper _testOutputHelper;
            private readonly IEventStore _eventStore;
            private readonly SqlConnection _connection;
            private SqlCommand _command;
            private Checkpoint _checkpoint;

            public SqlWatcher(string connectionString, ITestOutputHelper testOutputHelper, IEventStore eventStore,
                Checkpoint checkpoint)
            {
                _connectionString = connectionString;
                _testOutputHelper = testOutputHelper;
                _eventStore = eventStore;
                _checkpoint = checkpoint;
                _connection = new SqlConnection(_connectionString);
                SqlDependency.Start(connectionString);
            }

            public async Task Start()
            {
                await _connection.OpenAsync();

                await GetData();
            }

            private async Task GetData()
            {
                _command = new SqlCommand("SELECT Id, Ordinal FROM dbo.Events ORDER By Ordinal DESC", _connection);
                _command.Notification = null;
                var sqlDependency = new SqlDependency(_command);
                sqlDependency.OnChange += SqlDependencyOnOnChange;

                var reader = await _command.ExecuteReaderAsync();

                _testOutputHelper.WriteLine(reader.HasRows.ToString());
                _testOutputHelper.WriteLine(reader.RecordsAffected.ToString());

                while(await reader.ReadAsync())
                {
                    _testOutputHelper.WriteLine(reader["Id"].ToString());
                    _testOutputHelper.WriteLine(reader["Ordinal"].ToString());
                }
            }

            private void SqlDependencyOnOnChange(object sender, SqlNotificationEventArgs sqlNotificationEventArgs)
            {
                SqlDependency dependency = (SqlDependency)sender;
                dependency.OnChange -= SqlDependencyOnOnChange;

                _testOutputHelper.WriteLine(sqlNotificationEventArgs.Source.ToString());
                _testOutputHelper.WriteLine(sqlNotificationEventArgs.Info.ToString());
                _testOutputHelper.WriteLine(sqlNotificationEventArgs.Type.ToString());

                Task.Run(GetData);
            }

            public void Dispose()
            {
                _command.Dispose();
                SqlConnection.ClearPool(_connection);
                _connection.Dispose();
                SqlDependency.Stop(_connectionString);
            }
        }
    }
}