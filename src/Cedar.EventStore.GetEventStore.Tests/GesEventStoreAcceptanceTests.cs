namespace Cedar.EventStore
{
    using System;
    using Xunit.Abstractions;

    public partial class EventStoreAcceptanceTests
    {
        private EventStoreAcceptanceTestFixture GetFixture()
        {
            return new GesEventStoreFixture();
        }

        private IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return new Disposable();
        }

        private class Disposable : IDisposable
        {
            public void Dispose()
            {
                
            }
        }
    }
}