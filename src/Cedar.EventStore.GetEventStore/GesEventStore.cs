﻿namespace Cedar.EventStore
 {
     using System;
     using System.Collections.Concurrent;
     using System.Collections.Generic;
     using System.Collections.ObjectModel;
     using System.Linq;
     using System.Text;
     using System.Threading.Tasks;
     using EnsureThat;
     using global::EventStore.ClientAPI;

     public class GesEventStore : IEventStore
     {
         private readonly IJsonSerializer _jsonSerializer;
         private readonly ConcurrentDictionary<string, IEventStoreConnection> _connectionCache 
             = new ConcurrentDictionary<string, IEventStoreConnection>(); 
         private readonly CreateEventStoreConnection _getConnection;
         private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();

         public GesEventStore(CreateEventStoreConnection createConnection, IJsonSerializer jsonSerializer = null)
         {
             Ensure.That(createConnection, "connectionFactory").IsNotNull();

             _getConnection = storeid =>
             {
                 if(_isDisposed.Value)
                 {
                     throw new ObjectDisposedException("GesEventStore");
                 }
                 return _connectionCache.GetOrAdd(storeid, key => createConnection(key));
             };

             _jsonSerializer = jsonSerializer ?? DefaultJsonSerializer.Instance;
         }

         public async Task AppendToStream(string storeId, string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
         {
             var connection = _getConnection(storeId);

             var eventDatas = events.Select(e =>
             {
                 var json = _jsonSerializer.Serialize(e.Data);
                 var data = Encoding.UTF8.GetBytes(json);
                 return new EventData(e.EventId, "type", true, data, e.Metadata.ToArray());
             });

             try
             {
                 await connection
                     .AppendToStreamAsync(streamId, expectedVersion, eventDatas)
                     .NotOnCapturedContext();
             }
             catch(global::EventStore.ClientAPI.Exceptions.StreamDeletedException ex)
             {
                 throw new StreamDeletedException(storeId, streamId, ex);
             }
         }

         public Task DeleteStream(
             string storeId,
             string streamId,
             int exptectedVersion = ExpectedVersion.Any)
         {
             var connection = _getConnection(storeId);
             return connection.DeleteStreamAsync(streamId, exptectedVersion, hardDelete: true);
         }

         public async Task<AllEventsPage> ReadAll(
             string storeId,
             string checkpoint,
             int maxCount,
             ReadDirection direction = ReadDirection.Forward)
         {
             var connection = _getConnection(storeId);

             var position = checkpoint.ParsePosition() ?? Position.Start;

             AllEventsSlice allEventsSlice;
             if (direction == ReadDirection.Forward)
             {
                 allEventsSlice = await connection
                     .ReadAllEventsForwardAsync(position, maxCount, resolveLinkTos: false)
                     .NotOnCapturedContext();
             }
             else
             {
                 allEventsSlice = await connection
                     .ReadAllEventsBackwardAsync(position, maxCount, resolveLinkTos: false)
                     .NotOnCapturedContext();
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
             var connection = _getConnection(storeId);

             StreamEventsSlice streamEventsSlice;
             if (direction == ReadDirection.Forward)
             {
                 streamEventsSlice = await connection
                     .ReadStreamEventsForwardAsync(streamId, start, count, true)
                     .NotOnCapturedContext();
             }
             else
             {
                 streamEventsSlice = await connection
                     .ReadStreamEventsBackwardAsync(streamId, start, count, true)
                     .NotOnCapturedContext();
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
             if(_isDisposed.EnsureCalledOnce())
             {
                 return;
             }
             foreach(var eventStoreConnection in _connectionCache.Values)
             {
                 eventStoreConnection.Dispose();
             }
             _connectionCache.Clear();
         }

         private ReadDirection GetReadDirection(global::EventStore.ClientAPI.ReadDirection readDirection)
         {
             return (ReadDirection)Enum.Parse(typeof(ReadDirection), readDirection.ToString());
         }
     }
 }