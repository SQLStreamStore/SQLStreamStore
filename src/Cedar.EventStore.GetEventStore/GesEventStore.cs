﻿namespace Cedar.EventStore
 {
     using System;
     using System.Collections.Generic;
     using System.Linq;
     using System.Threading.Tasks;
     using EnsureThat;
     using global::EventStore.ClientAPI;

     public class GesEventStore : IEventStore
     {
         private readonly IEventStoreConnection _connection;

         public GesEventStore(Func<IEventStoreConnection> createConnection)
         {
             Ensure.That(createConnection, "connectionFactory").IsNotNull();

             _connection = createConnection();
         }

         public Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
         {
             var eventDatas = events.Select(e => new EventData(e.EventId, "type", false, e.Body.ToArray(), e.Metadata.ToArray()));

             return _connection.AppendToStreamAsync(streamId, expectedVersion, eventDatas);
         }

         public Task DeleteStream(string streamId, int exptectedVersion = EventStore.ExpectedVersion.Any, bool hardDelete = true)
         {
             return _connection.DeleteStreamAsync(streamId, exptectedVersion, hardDelete);
         }

         public async Task<AllEventsPage> ReadAll(
             string checkpoint,
             int maxCount,
             ReadDirection direction = ReadDirection.Forward)
         {
             var position = checkpoint.ParsePosition() ?? Position.Start;

             AllEventsSlice allEventsSlice;
             if (direction == ReadDirection.Forward)
             {
                 allEventsSlice = await _connection.ReadAllEventsForwardAsync(position, maxCount, true);
             }
             else
             {
                 allEventsSlice = await _connection.ReadAllEventsBackwardAsync(position, maxCount, true);
             }

             throw new NotImplementedException();
         }

         public async Task<StreamEventsPage> ReadStream(
             string streamId,
             int start,
             int count,
             EventStore.ReadDirection direction = EventStore.ReadDirection.Forward)
         {
             StreamEventsSlice streamEventsSlice;
             if (direction == EventStore.ReadDirection.Forward)
             {
                 streamEventsSlice = await _connection.ReadStreamEventsForwardAsync(streamId, start, count, true);
             }
             else
             {
                 streamEventsSlice = await _connection.ReadStreamEventsBackwardAsync(streamId, start, count, true);
             }

             return new StreamEventsPage(
                 streamId,
                 (PageReadStatus)Enum.Parse(typeof(PageReadStatus), streamEventsSlice.Status.ToString()),
                 streamEventsSlice.FromEventNumber,
                 streamEventsSlice.NextEventNumber,
                 streamEventsSlice.LastEventNumber,
                 (ReadDirection)Enum.Parse(typeof(ReadDirection), streamEventsSlice.ReadDirection.ToString()),
                 streamEventsSlice.IsEndOfStream, streamEventsSlice
                     .Events
                     .Select(e => new StreamEvent(
                         streamId,
                         e.Event.EventId,
                         e.Event.EventNumber,
                         e.OriginalPosition.ToCheckpoint(),
                         e.Event.Data,
                         e.Event.Metadata))
                     .ToArray());
         }

         public void Dispose()
         {
             _connection.Dispose();
         }
     }
 }