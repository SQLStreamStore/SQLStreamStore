namespace SqlStreamStore.Oracle.Tests
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using global::Oracle.ManagedDataAccess.Client;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Logging;
    using SqlStreamStore.Streams;
    using Xunit;
    using Xunit.Abstractions;

    public class OracleAcceptanceTests: AcceptanceTests
    {
        public OracleAcceptanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        { }

        [Fact]
        public async Task Time_to_take_to_read_1000_read_head_positions()
        {
            var store = Fixture.Store;
            await store.AppendToStream("stream-1", ExpectedVersion.NoStream, CreateNewStreamMessages(1, 2, 3));

            var stopwatch = Stopwatch.StartNew();

            for(int i = 0; i < 1000; i++)
            {
                await store.ReadHeadPosition();
            }

            TestOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
        }
        
        [Theory]
        [InlineData(1000)]
        [InlineData(5000)]
        public async Task Time_to_take_to_write_ParallelWrite_3_messages_to_1000_streams_NoStream(int numStreams)
        {
            LogProvider.IsDisabled = true;
                
            var store = Fixture.Store;
            
            var stopwatch = Stopwatch.StartNew();

            List<Task> parallelTasks = new List<Task>();
            
            for(int i = 0; i < numStreams; i++)
            {
                parallelTasks.Add( Task.Run(async () =>
                {
                    await store.AppendToStream("stream-" + i,  ExpectedVersion.NoStream,
                        CreateNewStreamMessages(1, 2, 3, 4, 5, 6));
                }));
            }

            Task.WaitAll(parallelTasks.ToArray());

            stopwatch.Stop();

            var elapsed = stopwatch.ElapsedMilliseconds;
            var elapsedPerStream = elapsed / numStreams;
            
            TestOutputHelper.WriteLine($"{elapsed} for {numStreams} streams");
            TestOutputHelper.WriteLine($"{elapsedPerStream} / stream");
        }
        
        [Fact]
        public async Task Time_to_take_to_write_ParallelWrite_3_messages_to_1000_streams_AnyVersion()
        {
            var store = Fixture.Store;
            

            var stopwatch = Stopwatch.StartNew();

            List<Task> parallelTasks = new List<Task>();
            
            for(int i = 0; i < 1000; i++)
            {
                parallelTasks.Add( Task.Run(async () =>
                {
                     await store.AppendToStream("stream-" + i,  ExpectedVersion.Any,
                            CreateNewStreamMessages(1, 2, 3));
                }));
            }

            Task.WaitAll(parallelTasks.ToArray());

            TestOutputHelper.WriteLine(stopwatch.ElapsedMilliseconds.ToString());
        }
        
        protected override async Task<IStreamStoreFixture> CreateFixture()
        {
            var connString = "user id=REL;password=REL;data source=localhost:41521/ORCLCDB;" +
                            "Min Pool Size=50;Connection Lifetime=120;Connection Timeout=60;" +
                            "Incr Pool Size=5; Decr Pool Size=2";

            return await OracleFixture.CreateFixture(connString);
        }

        
    }

    public class OracleFixture : IStreamStoreFixture
    {
        private readonly string _connectionstring;

        public OracleFixture(string connString)
        {
            _connectionstring = connString;
        }

        public void Dispose()
        { }

        public IStreamStore Store
        {
            get
            {
                var settings = new OracleStreamStoreSettings("REL", _connectionstring)
                {
                    DisableDeletionTracking = DisableDeletionTracking,
                    GetUtcNow = GetUtcNow
                };

                return new OracleStreamStore(settings);
            }
        }

        public GetUtcNow GetUtcNow { get; set; } = SystemClock.GetUtcNow;
        public long MinPosition { get; set; }
        public int MaxSubscriptionCount { get; set; }
        public bool DisableDeletionTracking { get; set; }

        public static async Task<OracleFixture> CreateFixture(string connectionstring)
        {
            using(var conn = new OracleConnection(connectionstring))
            {
                await conn.OpenAsync();

                using(var cmd = new OracleCommand("DELETE FROM STREAMEVENTS WHERE 1=1", conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                using(var cmd = new OracleCommand("DELETE FROM STREAMS WHERE 1=1", conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                using(var cmd =
                    new OracleCommand(
                        "ALTER TABLE STREAMS MODIFY (IDINTERNAL NUMBER(10) GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1))",
                        conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

                using(var cmd =
                    new OracleCommand(
                        "ALTER TABLE STREAMEVENTS MODIFY (POSITION INT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1 MINVALUE 0))",
                        conn))
                {
                    await cmd.ExecuteNonQueryAsync();
                }

            }

            return new OracleFixture(connectionstring);
        }

    }
}