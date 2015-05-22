﻿namespace Cedar.EventStore
 {
     using System;
     using System.Collections.Generic;
     using System.Linq;
     using System.Text;
     using System.Threading;
     using System.Threading.Tasks;
     using Cedar.EventStore.Exceptions;
     using EnsureThat;
     using global::EventStore.ClientAPI;

     public class GesEventStore : IEventStore
     {
         private readonly CreateEventStoreConnection _getConnection;
         private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();
         private readonly IEventStoreConnection _connection;

         public GesEventStore(CreateEventStoreConnection createConnection)
         {
             Ensure.That(createConnection, "createConnection").IsNotNull();

             _connection = createConnection();
             _getConnection = () =>
             {
                 if(_isDisposed.Value) 
                 {
                     throw new ObjectDisposedException("GesEventStore");
                 }
                 return _connection;
             };
         }

         public async Task AppendToStream(
             string streamId,
             int expectedVersion,
             IEnumerable<NewStreamEvent> events,
             CancellationToken cancellationToken = default(CancellationToken))
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

         public async Task DeleteStream(
             string streamId,
             int exptectedVersion = ExpectedVersion.Any,
             CancellationToken cancellationToken = default(CancellationToken))
         {
             var connection = _getConnection();

             try
             {
                 await connection
                     .DeleteStreamAsync(streamId, exptectedVersion, hardDelete: true)
                     .NotOnCapturedContext();
             }
             catch(global::EventStore.ClientAPI.Exceptions.WrongExpectedVersionException ex)
             {
                 throw new WrongExpectedVersionException(streamId, exptectedVersion, ex);
             }
         }

         public async Task<AllEventsPage> ReadAll(
             Checkpoint checkpoint,
             int maxCount,
             ReadDirection direction = ReadDirection.Forward,
             CancellationToken cancellationToken = default(CancellationToken))
         {
             Ensure.That(checkpoint, "checkpoint").IsNotNull();

             var connection = _getConnection();

             var position = checkpoint.ParsePosition();

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
                 .Select(resolvedEvent =>resolvedEvent.ToSteamEvent())
                 .ToArray();

             return new AllEventsPage(
                 allEventsSlice.FromPosition.ToString(),
                 allEventsSlice.NextPosition.ToString(),
                 allEventsSlice.IsEndOfStream,
                 GetReadDirection(allEventsSlice.ReadDirection),
                 events);
         }

         public async Task<StreamEventsPage> ReadStream(
             string streamId,
             int start,
             int count,
             ReadDirection direction = ReadDirection.Forward,
             CancellationToken cancellationToken = default(CancellationToken))
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
                     .Select(resolvedEvent => resolvedEvent.ToSteamEvent())
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