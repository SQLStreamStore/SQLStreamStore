 // ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.InMemory;
    using Xunit.Abstractions;

    public partial class StreamStoreAcceptanceTests
    {
        private StreamStoreAcceptanceTestFixture GetFixture()
        {
            return new InMemoryStreamStoreFixture();
        }

        private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }
    }
}