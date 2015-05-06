﻿namespace Cedar.EventStore
 {
     using System;
     using System.Collections.Generic;
     using System.Collections.ObjectModel;
     using System.Linq;
     using System.Text;
     using System.Threading.Tasks;
     using EnsureThat;
     using global::EventStore.ClientAPI;

     public class GesEventStore : IEventStore
     {
         private readonly ISerializer _serializer;
         private readonly IEventStoreConnection _connection;

         public GesEventStore(Func<IEventStoreConnection> createConnection, ISerializer serializer = null)
         {
             Ensure.That(createConnection, "connectionFactory").IsNotNull();

             _serializer = serializer ?? DefaultJsonSerializer.Instance;
             _connection = createConnection();
         }

         public Task AppendToStream(string storeId, string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
         {
             StoreIdMustBeDefault(storeId);

             var eventDatas = events.Select(e =>
             {
                 var json = _serializer.Serialize(e.Data);
                 var data = Encoding.UTF8.GetBytes(json);
                 return new EventData(e.EventId, "type", true, data, e.Metadata.ToArray());
             });

             return _connection.AppendToStreamAsync(streamId, expectedVersion, eventDatas);
         }

         public Task DeleteStream(string storeId, string streamId, int exptectedVersion = ExpectedVersion.Any, bool hardDelete = true)
         {
             StoreIdMustBeDefault(storeId);

             return _connection.DeleteStreamAsync(streamId, exptectedVersion, hardDelete);
         }

         public async Task<AllEventsPage> ReadAll(
             string storeId,
             string checkpoint,
             int maxCount,
             ReadDirection direction = ReadDirection.Forward)
         {
             StoreIdMustBeDefault(storeId);

             var position = checkpoint.ParsePosition() ?? Position.Start;

             AllEventsSlice allEventsSlice;
             if (direction == ReadDirection.Forward)
             {
                 allEventsSlice = await _connection.ReadAllEventsForwardAsync(position, maxCount, false);
             }
             else
             {
                 allEventsSlice = await _connection.ReadAllEventsBackwardAsync(position, maxCount, false);
             }

             var events = allEventsSlice
                 .Events
                 .Where(@event => 
                     !(@event.OriginalEvent.EventType.StartsWith("$") 
                     || @event.OriginalStreamId.StartsWith("$")))
                 .Select(@event =>
                     new StreamEvent(storeId,
                         @event.OriginalStreamId,
                         @event.Event.EventId,
                         @event.Event.EventNumber,
                         @event.OriginalPosition.ToCheckpoint(),
                         Encoding.UTF8.GetString(@event.Event.Data),
                         new ReadOnlyCollection<byte>(@event.Event.Metadata)))
                 .ToArray();

             return new AllEventsPage(
                 allEventsSlice.FromPosition.ToString(),
                 allEventsSlice.NextPosition.ToString(),
                 allEventsSlice.IsEndOfStream,
                 GetReadDirection(allEventsSlice.ReadDirection),
                 new ReadOnlyCollection<StreamEvent>(events));
         }

         public async Task<StreamEventsPage> ReadStream(
             string storeId,
             string streamId,
             int start,
             int count,
             ReadDirection direction = ReadDirection.Forward)
         {
             StoreIdMustBeDefault(storeId);

             StreamEventsSlice streamEventsSlice;
             if (direction == ReadDirection.Forward)
             {
                 streamEventsSlice = await _connection.ReadStreamEventsForwardAsync(streamId, start, count, true);
             }
             else
             {
                 streamEventsSlice = await _connection.ReadStreamEventsBackwardAsync(streamId, start, count, true);
             }

             return new StreamEventsPage(
                 DefaultStore.StoreId,
                 streamId,
                 (PageReadStatus)Enum.Parse(typeof(PageReadStatus), streamEventsSlice.Status.ToString()),
                 streamEventsSlice.FromEventNumber,
                 streamEventsSlice.NextEventNumber,
                 streamEventsSlice.LastEventNumber,
                 GetReadDirection(streamEventsSlice.ReadDirection),
                 streamEventsSlice.IsEndOfStream, streamEventsSlice
                     .Events
                     .Select(e => new StreamEvent(
                         storeId,
                         streamId,
                         e.Event.EventId,
                         e.Event.EventNumber,
                         e.OriginalPosition.ToCheckpoint(),
                         Encoding.UTF8.GetString(e.Event.Data),
                         e.Event.Metadata))
                     .ToArray());
         }

         public void Dispose()
         {
             _connection.Dispose();
         }

         private ReadDirection GetReadDirection(global::EventStore.ClientAPI.ReadDirection readDirection)
         {
             return (ReadDirection)Enum.Parse(typeof(ReadDirection), readDirection.ToString());
         }

         private void StoreIdMustBeDefault(string storeId)
         {
             if (!storeId.Equals(DefaultStore.StoreId, StringComparison.Ordinal))
             {
                 throw new NotSupportedException("Get EventStore v3.0 doesn't support multi-tenancy (yet)");
             }
         }
     }
 }