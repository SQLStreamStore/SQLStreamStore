namespace SqlStreamStore
{
    using System.Threading.Tasks;
    using Xunit;
    using Xunit.Abstractions;

    public class SqliteStreamStoreAcceptanceTests : AcceptanceTests, IClassFixture<SqliteStreamStoreFixture>
    {
        private readonly Task<SqliteStreamStoreFixture> _fixture;
        
        public SqliteStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper, SqliteStreamStoreFixture fixture)
            : base(testOutputHelper)
        {
            _fixture = Task.FromResult<SqliteStreamStoreFixture>(fixture);
        }

        protected override async Task<IStreamStoreFixture> CreateFixture()
        {
            var fixture = await _fixture;
            await fixture.Prepare();
            return fixture;
        }
    }
}