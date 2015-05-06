namespace Cedar.EventStore
{
    using System;
    using System.Linq;

    public abstract partial class EventStoreAcceptanceTests
    {
        protected abstract EventStoreAcceptanceTestFixture GetFixture();

        private static NewStreamEvent[] CreateNewStreamEvents(params int[] eventNumbers)
        {
            return eventNumbers
                .Select(eventNumber =>
                {
                    var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
                    return new NewStreamEvent(eventId, "data", new byte[] { 3, 4 });
                })
                .ToArray();
        }

        private static StreamEvent ExpectedStreamEvent(
            string storeId,
            string streamId,
            int eventNumber,
            int sequenceNumber)
        {
            var eventId = Guid.Parse("00000000-0000-0000-0000-" + eventNumber.ToString().PadLeft(12, '0'));
            return new StreamEvent(storeId, streamId, eventId, sequenceNumber, null, "\"data\"", new byte[] { 3, 4 });
        }
    }
}