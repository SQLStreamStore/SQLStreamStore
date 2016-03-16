 // ReSharper disable once CheckNamespace
namespace Cedar.EventStore
{
    using System;
    using Cedar.EventStore.InMemory;
    using Xunit.Abstractions;

    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture()
        {
            return new InMemoryEventStoreFixture();
        }

        private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }
    }
}