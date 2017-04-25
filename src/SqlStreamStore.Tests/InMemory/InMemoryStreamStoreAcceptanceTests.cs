 // ReSharper disable once CheckNamespace
namespace SqlStreamStore
{
    using System;
    using SqlStreamStore.InMemory;
    using Xunit.Abstractions;

    public class InMemoryStreamStoreAcceptanceTests : StreamStoreAcceptanceTests
    {
        public InMemoryStreamStoreAcceptanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        protected override StreamStoreAcceptanceTestFixture GetFixture()
        {
            return new InMemoryStreamStoreFixture();
        }

        protected override IDisposable CaptureLogs(ITestOutputHelper testOutputHelper)
        {
            return LoggingHelper.Capture(testOutputHelper);
        }
    }
}