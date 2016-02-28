namespace Cedar.EventStore
{
    using Xunit.Abstractions;

    public class ExploratoryTests
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public ExploratoryTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }
    }
}