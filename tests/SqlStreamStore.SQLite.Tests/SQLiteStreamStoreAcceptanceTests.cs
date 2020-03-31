namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class SQLiteStreamStoreAcceptanceTests : AcceptanceTests, IClassFixture<SQLiteStreamStoreFixture>
    {
        private readonly SQLiteStreamStoreFixture _fixture;

        public SQLiteStreamStoreAcceptanceTests(SQLiteStreamStoreFixture fixture, ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
            _fixture = fixture;
        }

        protected override Task<IStreamStoreFixture> CreateFixture()
            => Task.FromResult<IStreamStoreFixture>(_fixture);
    }
}