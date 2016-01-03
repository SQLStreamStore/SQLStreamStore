namespace Cedar.EventStore
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Cedar.EventStore.Infrastructure;
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


                    using (var watcher = new SqlWatcher(fixture.ConnectionString, _testOutputHelper, eventStore))
                    {
                        await watcher.Start();

                        await createStreams(10, 100);
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
            private LongCheckpoint _checkpoint;

            public SqlWatcher(string connectionString, ITestOutputHelper testOutputHelper, IEventStore eventStore)
            {
                _connectionString = connectionString;
                _testOutputHelper = testOutputHelper;
                _eventStore = eventStore;
                _connection = new SqlConnection(_connectionString);
                SqlDependency.Start(connectionString);
            }

            public async Task Start()
            {
                var allEventsPage = await _eventStore.ReadAll(_eventStore.EndCheckpoint, 1, ReadDirection.Backward);

                _checkpoint = LongCheckpoint.Parse(allEventsPage.NextCheckpoint);

                await _connection.OpenAsync();

                await GetData();
            }

            private async Task GetData()
            {
                _command = new SqlCommand("SELECT Id, Ordinal FROM dbo.Events", _connection)
                {
                    Notification = null
                };
                var sqlDependency = new SqlDependency(_command);
                sqlDependency.OnChange += SqlDependencyOnOnChange;

                await _command.ExecuteNonQueryAsync();

                var allEventsPage = await _eventStore.ReadAll(_checkpoint.Value, int.MaxValue);

                _testOutputHelper.WriteLine(allEventsPage.FromCheckpoint);
                _testOutputHelper.WriteLine(allEventsPage.NextCheckpoint);
                _checkpoint = LongCheckpoint.Parse(allEventsPage.NextCheckpoint);

                /*
                var reader = await _command.ExecuteReaderAsync(();
                 _testOutputHelper.WriteLine(reader.HasRows.ToString());
                 _testOutputHelper.WriteLine(reader.RecordsAffected.ToString());

                 while(await reader.ReadAsync())
                 {
                     _testOutputHelper.WriteLine(reader["Id"].ToString());
                     _testOutputHelper.WriteLine(reader["Ordinal"].ToString());
                 }*/
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