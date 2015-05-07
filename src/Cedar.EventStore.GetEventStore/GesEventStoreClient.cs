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

     public class GesEventStoreClient : IEventStoreClient
     {
         private readonly CreateEventStoreConnection _getConnection;
         private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();
         private readonly IEventStoreConnection _connection;

         public GesEventStoreClient(CreateEventStoreConnection createConnection)
         {
             Ensure.That(createConnection, "connectionFactory").IsNotNull();

             _connection = createConnection();
             _getConnection = () =>
             {
                 if(_isDisposed.Value)
                 {
                     throw new ObjectDisposedException("GesEventStoreClient");
                 }
                 return _connection;
             };
         }

         public async Task AppendToStream(string streamId, int expectedVersion, IEnumerable<NewStreamEvent> events)
         {
             var connection = _getConnection();

             var eventDatas = events.Select(e =>
             {
                 var data = Encoding.UTF8.GetBytes(e.JsonData);
                 var metadata = Encoding.UTF8.GetBytes(e.JsonMetadata);

                 return new EventData(e.EventId, e.Type, true, data, metadata);
             });

             try
             {
                 await connection
                     .AppendToStreamAsync(streamId, expectedVersion, eventDatas)
                     .NotOnCapturedContext();
             }
             catch(global::EventStore.ClientAPI.Exceptions.StreamDeletedException ex)
             {
                 throw new StreamDeletedException(streamId, ex);
             }
         }

         public Task DeleteStream(
             string streamId,
             int exptectedVersion = ExpectedVersion.Any)
         {
             var connection = _getConnection();
             return connection.DeleteStreamAsync(streamId, exptectedVersion, hardDelete: true);
         }

         public async Task<AllEventsPage> ReadAll(
             string checkpoint,
             int maxCount,
             ReadDirection direction = ReadDirection.Forward)
         {
             var connection = _getConnection();

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
                 .Select(resolvedEvent =>
                     new StreamEvent(
                         resolvedEvent.Event.EventStreamId,
                         resolvedEvent.Event.EventId,
                         resolvedEvent.Event.EventNumber,
                         resolvedEvent.OriginalPosition.ToCheckpoint(),
                         resolvedEvent.Event.EventType,
                         Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                         Encoding.UTF8.GetString(resolvedEvent.Event.Metadata)))
                 .ToArray();

             return new AllEventsPage(
                 allEventsSlice.FromPosition.ToString(),
                 allEventsSlice.NextPosition.ToString(),
                 allEventsSlice.IsEndOfStream,
                 GetReadDirection(allEventsSlice.ReadDirection),
                 new ReadOnlyCollection<StreamEvent>(events));
         }

         public async Task<StreamEventsPage> ReadStream(
             string streamId,
             int start,
             int count,
             ReadDirection direction = ReadDirection.Forward)
         {
             var connection = _getConnection();

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
                 streamId,
                 (PageReadStatus)Enum.Parse(typeof(PageReadStatus), streamEventsSlice.Status.ToString()),
                 streamEventsSlice.FromEventNumber,
                 streamEventsSlice.NextEventNumber,
                 streamEventsSlice.LastEventNumber,
                 GetReadDirection(streamEventsSlice.ReadDirection),
                 streamEventsSlice.IsEndOfStream, streamEventsSlice
                     .Events
                     .Select(resolvedEvent => new StreamEvent(
                         resolvedEvent.Event.EventStreamId,
                         resolvedEvent.Event.EventId,
                         resolvedEvent.Event.EventNumber,
                         resolvedEvent.OriginalPosition.ToCheckpoint(),
                         resolvedEvent.Event.EventType,
                         Encoding.UTF8.GetString(resolvedEvent.Event.Data),
                         Encoding.UTF8.GetString(resolvedEvent.Event.Metadata)))
                     .ToArray());
         }

         public void Dispose()
         {
             if(_isDisposed.EnsureCalledOnce())
             {
                 return;
             }
             _connection.Dispose();
         }

         private ReadDirection GetReadDirection(global::EventStore.ClientAPI.ReadDirection readDirection)
         {
             return (ReadDirection)Enum.Parse(typeof(ReadDirection), readDirection.ToString());
         }
     }
 }