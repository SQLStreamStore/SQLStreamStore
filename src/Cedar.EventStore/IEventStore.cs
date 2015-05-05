using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cedar.EventStore
{
    public interface IEventStore : IDisposable
    {
        Task AppendToStream(string storeId, string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events);

        Task DeleteStream(string storeId, string streamId, int expectedVersion = ExpectedVersion.Any, bool hardDelete = true);

        Task<AllEventsPage> ReadAll(string storeId, string checkpoint, int maxCount, ReadDirection direction = ReadDirection.Forward);

        Task<StreamEventsPage> ReadStream(string storeId, string streamId, int start, int count, ReadDirection direction = ReadDirection.Forward);
    }
}
