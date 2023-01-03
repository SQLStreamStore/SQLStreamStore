namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using SqlStreamStore.Streams;

    internal static class MessageFactory
    {
        public static NewStreamMessage[] CreateNewStreamMessages(params int[] messageNumbers) 
            => CreateNewStreamMessages("\"data\"", messageNumbers);

        public static NewStreamMessage[] CreateNewStreamMessages(string jsonData, params int[] messageNumbers)
            => messageNumbers
                .Select(number =>
                {
                    var id = Guid.Parse("00000000-0000-0000-0000-" + number.ToString().PadLeft(12, '0'));
                    return new NewStreamMessage(id, "type", jsonData, "\"metadata\"");
                })
                .ToArray();

        public static NewStreamMessage[] CreateNewStreamMessageSequence(int startId, int count)
        {
            var messages = new List<NewStreamMessage>();
            for(int i = 0; i < count; i++)
            {
                var messageNumber = startId + i;
                var messageId = Guid.Parse("00000000-0000-0000-0000-" + messageNumber.ToString().PadLeft(12, '0'));
                var newStreamMessage = new NewStreamMessage(messageId, "type", "\"data\"", "\"metadata\"");
                messages.Add(newStreamMessage);
            }
            return messages.ToArray();
        }

        public static StreamMessage ExpectedStreamMessage(
            string streamId,
            int messageNumber,
            int sequenceNumber,
            DateTime created)
        {
            var id = Guid.Parse("00000000-0000-0000-0000-" + messageNumber.ToString().PadLeft(12, '0'));
            return new StreamMessage(streamId, id, sequenceNumber, 0, created, "type", "\"metadata\"", "\"data\"");
        }
    }
}