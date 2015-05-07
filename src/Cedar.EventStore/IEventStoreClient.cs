using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cedar.EventStore
{
    public interface IEventStoreClient : IDisposable
    {
        Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events);

        Task DeleteStream(string streamId, int expectedVersion = ExpectedVersion.Any);

        Task<AllEventsPage> ReadAll(string checkpoint, int maxCount, ReadDirection direction = ReadDirection.Forward);

        Task<StreamEventsPage> ReadStream(string streamId, int start, int count, ReadDirection direction = ReadDirection.Forward);

        // TODO subscriptions
    }
}
