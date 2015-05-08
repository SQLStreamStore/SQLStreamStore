namespace Cedar.EventStore.MsSql.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

    public class MsSqlEventStoreTests : EventStoreAcceptanceTests
    {
        protected override EventStoreAcceptanceTestFixture GetFixture()
        {
            return new MsSqlEventStoreFixture();
        }
    }

    public class MsSqlEventStoreFixture : EventStoreAcceptanceTestFixture
    {
        public override async Task<IEventStore> GetEventStore()
        {
            Func<SqlConnection> createConnectionFunc = () => new SqlConnection(
                @"Data Source=(LocalDB)\v11.0; Integrated Security=True; MultipleActiveResultSets=True");

            var eventStore = new MsSqlEventStore(createConnectionFunc);
            await eventStore.InitializeStore(ignoreErrors: true);

            return new EventStoreWrapper(eventStore, () => eventStore.DropAll(ignoreErrors: true).Wait());
        }

        private class EventStoreWrapper : IEventStore
        {
            private readonly IEventStore _inner;
            private readonly Action _onDispose;

            public EventStoreWrapper(IEventStore inner, Action onDispose)
            {
                _inner = inner;
                _onDispose = onDispose;
            }

            public void Dispose()
            {
                _inner.Dispose();
                _onDispose();
            }

            public Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
            {
                return _inner.AppendToStream(streamId, expectedVersion, events);
            }

            public Task DeleteStream(string streamId, int expectedVersion = ExpectedVersion.Any)
            {
                return _inner.DeleteStream(streamId, expectedVersion);
            }

            public Task<AllEventsPage> ReadAll(
                string checkpoint,
                int maxCount,
                ReadDirection direction = ReadDirection.Forward)
            {
                return _inner.ReadAll(checkpoint, maxCount, direction);
            }

            public Task<StreamEventsPage> ReadStream(
                string streamId,
                int start,
                int count,
                ReadDirection direction = ReadDirection.Forward)
            {
                return _inner.ReadStream(streamId, start, count, direction);
            }
        }
    }
}