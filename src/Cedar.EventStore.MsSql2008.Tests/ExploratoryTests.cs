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
                    var newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                    await eventStore.AppendToStream("stream-1", ExpectedVersion.NoStream, newStreamEvent);

                    newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                    await eventStore.AppendToStream("stream-2", ExpectedVersion.NoStream, newStreamEvent);

                    using (var watcher = new SqlWatcher(fixture.ConnectionString, _testOutputHelper))
                    {
                        await watcher.Start();

                        newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                        await eventStore.AppendToStream("stream-3", ExpectedVersion.NoStream, newStreamEvent);

                        await Task.Delay(100);

                        newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                        await eventStore.AppendToStream("stream-4", ExpectedVersion.NoStream, newStreamEvent);

                        newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                        await eventStore.AppendToStream("stream-5", ExpectedVersion.NoStream, newStreamEvent);

                        newStreamEvent = new NewStreamEvent(Guid.NewGuid(), "MyEventType", "{}");
                        await eventStore.AppendToStream("stream-6", ExpectedVersion.NoStream, newStreamEvent);

                        await Task.Delay(100);
                    }
                }
            }
        }

        private class SqlWatcher : IDisposable
        {
            private readonly string _connectionString;
            private readonly ITestOutputHelper _testOutputHelper;
            private readonly SqlConnection _connection;
            private SqlCommand _command;

            public SqlWatcher(string connectionString, ITestOutputHelper testOutputHelper)
            {
                _connectionString = connectionString;
                _testOutputHelper = testOutputHelper;
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
                _command = new SqlCommand("SELECT Id, Ordinal FROM dbo.Events", _connection);
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
                _connection.Dispose();
                SqlDependency.Stop(_connectionString);
            }
        }
    }
}