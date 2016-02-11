﻿namespace Cedar.EventStore
﻿{
﻿    using System;
﻿    using System.Linq;
﻿    using System.Text;
﻿    using System.Threading;
﻿    using System.Threading.Tasks;
﻿    using Cedar.EventStore.Infrastructure;
﻿    using Cedar.EventStore.Streams;
﻿    using Cedar.EventStore.Subscriptions;
﻿    using EnsureThat;
﻿    using global::EventStore.ClientAPI;
﻿    using static Cedar.EventStore.Infrastructure.TaskHelpers;
﻿    using ExpectedVersion = Cedar.EventStore.Streams.ExpectedVersion;
﻿    using ReadDirection = Cedar.EventStore.Streams.ReadDirection;

﻿    public sealed class GesEventStore : IEventStore
﻿    {
﻿        private readonly CreateEventStoreConnection _getConnection;
﻿        private readonly InterlockedBoolean _isDisposed = new InterlockedBoolean();
﻿        private readonly IEventStoreConnection _connection;
﻿       
﻿        public GesEventStore(CreateEventStoreConnection createConnection)
﻿        {
﻿            Ensure.That(createConnection, "createConnection").IsNotNull();

﻿            _connection = createConnection();
﻿            _getConnection = () =>
﻿            {
﻿                if(_isDisposed.Value)
﻿                {
﻿                    throw new ObjectDisposedException("GesEventStore");
﻿                }
﻿                return _connection;
﻿            };
﻿        }

﻿        public string StartCheckpoint => PositionCheckpoint.Start.Value;

﻿        public string EndCheckpoint => PositionCheckpoint.End.Value;

﻿        public async Task AppendToStream(
﻿            string streamId,
﻿            int expectedVersion,
﻿            NewStreamEvent[] events,
﻿            CancellationToken cancellationToken = default(CancellationToken))
﻿        {
            CheckIfDisposed();

            var connection = _getConnection();

﻿            var eventDatas = events.Select(e =>
﻿            {
﻿                var data = Encoding.UTF8.GetBytes(e.JsonData);
﻿                var metadata = Encoding.UTF8.GetBytes(e.JsonMetadata);

﻿                return new EventData(e.EventId, e.Type, true, data, metadata);
﻿            });

﻿            try
﻿            {
﻿                await connection
﻿                    .AppendToStreamAsync(streamId, expectedVersion, eventDatas)
﻿                    .NotOnCapturedContext();
﻿            }
﻿            catch(global::EventStore.ClientAPI.Exceptions.StreamDeletedException ex)
﻿            {
﻿                throw new StreamDeletedException(ex.Message, ex);
﻿            }
﻿            catch(global::EventStore.ClientAPI.Exceptions.WrongExpectedVersionException ex)
﻿            {
﻿                throw new WrongExpectedVersionException(ex.Message, ex);
﻿            }
﻿        }

﻿        public async Task DeleteStream(
﻿            string streamId,
﻿            int exptectedVersion = ExpectedVersion.Any,
﻿            CancellationToken cancellationToken = default(CancellationToken))
﻿        {
﻿            CheckIfDisposed();

﻿            var connection = _getConnection();

﻿            try
﻿            {
﻿                await connection
                    .DeleteStreamAsync(streamId, exptectedVersion, hardDelete: true)
﻿                    .NotOnCapturedContext();
﻿            }
﻿            catch(global::EventStore.ClientAPI.Exceptions.WrongExpectedVersionException ex)
﻿            {
﻿                throw new WrongExpectedVersionException(ex.Message, ex);
﻿            }
﻿        }

﻿        public async Task<AllEventsPage> ReadAll(string fromCheckpoint, int maxCount, ReadDirection direction = ReadDirection.Forward, CancellationToken cancellationToken = default(CancellationToken))
﻿        {
﻿            Ensure.That(fromCheckpoint, nameof(fromCheckpoint)).IsNotNull();
﻿            CheckIfDisposed();

﻿            var connection = _getConnection();

﻿            var position = PositionCheckpoint.Parse(fromCheckpoint).Position;

﻿            AllEventsSlice allEventsSlice;
﻿            if(direction == ReadDirection.Forward)
﻿            {
﻿                allEventsSlice = await connection
﻿                    .ReadAllEventsForwardAsync(position, maxCount, resolveLinkTos: false)
﻿                    .NotOnCapturedContext();
﻿            }
﻿            else
﻿            {
﻿                allEventsSlice = await connection
﻿                    .ReadAllEventsBackwardAsync(position, maxCount, resolveLinkTos: false)
﻿                    .NotOnCapturedContext();
﻿            }

﻿            var events = allEventsSlice
﻿                .Events
﻿                .Where(@event =>
﻿                    !(@event.OriginalEvent.EventType.StartsWith("$")
﻿                      || @event.OriginalStreamId.StartsWith("$")))
﻿                .Select(resolvedEvent => resolvedEvent.ToSteamEvent())
﻿                .ToArray();

﻿            return new AllEventsPage(
﻿                allEventsSlice.FromPosition.ToString(),
﻿                allEventsSlice.NextPosition.ToString(),
﻿                allEventsSlice.IsEndOfStream,
﻿                GetReadDirection(allEventsSlice.ReadDirection),
﻿                events);
﻿        }

﻿        public async Task<StreamEventsPage> ReadStream(
﻿            string streamId,
﻿            int start,
﻿            int count,
﻿            ReadDirection direction = ReadDirection.Forward,
﻿            CancellationToken cancellationToken = default(CancellationToken))
﻿        {
﻿            CheckIfDisposed();

﻿            var connection = _getConnection();

﻿            StreamEventsSlice streamEventsSlice;
﻿            if(direction == ReadDirection.Forward)
﻿            {
﻿                streamEventsSlice = await connection
﻿                    .ReadStreamEventsForwardAsync(streamId, start, count, true)
﻿                    .NotOnCapturedContext();
﻿            }
﻿            else
﻿            {
﻿                streamEventsSlice = await connection
﻿                    .ReadStreamEventsBackwardAsync(streamId, start, count, true)
﻿                    .NotOnCapturedContext();
﻿            }

﻿            return new StreamEventsPage(
﻿                streamId,
﻿                (PageReadStatus) Enum.Parse(typeof(PageReadStatus), streamEventsSlice.Status.ToString()),
﻿                streamEventsSlice.FromEventNumber,
﻿                streamEventsSlice.NextEventNumber,
﻿                streamEventsSlice.LastEventNumber,
﻿                GetReadDirection(streamEventsSlice.ReadDirection),
﻿                streamEventsSlice.IsEndOfStream,
﻿                streamEventsSlice
﻿                    .Events
﻿                    .Select(resolvedEvent => resolvedEvent.ToSteamEvent())
﻿                    .ToArray());
﻿        }

﻿        public async Task<IStreamSubscription> SubscribeToStream(
             string streamId,
             int startPosition,
             StreamEventReceived streamEventReceived,
             SubscriptionDropped subscriptionDropped,
             string name,
             CancellationToken cancellationToken = default(CancellationToken))
﻿        {
            streamEventReceived = FilterOutSystemEvents(streamEventReceived);
            subscriptionDropped = subscriptionDropped ?? ((_, __) => { });
            name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            Action<ResolvedEvent> eventAppeared = (resolvedEvent) =>
﻿            {
﻿                var task = Task.Run(async () =>
﻿                {
﻿                    await streamEventReceived(resolvedEvent.ToSteamEvent()).NotOnCapturedContext();
﻿                });

﻿                task.GetAwaiter().GetResult();
﻿            };

﻿            Action<SubscriptionDropReason, Exception> gesSubscriptionDropped =
﻿                (reason, exception) =>
﻿                {
﻿                    subscriptionDropped(reason.ToString(), exception);
﻿                };

﻿            if(startPosition == StreamVersion.End)
﻿            {
﻿                var subscription = await _connection.SubscribeToStreamAsync(
﻿                    streamId,
﻿                    true,
                    (_, resolvedEvent) => eventAppeared(resolvedEvent),
﻿                    (_, reason, exception) => gesSubscriptionDropped(reason, exception));

﻿                return new StreamSubscripton(subscription, name);
﻿            }
﻿            else
﻿            {
                // GES catchup subscription starts at supplied startposition + 1;
                // So if you want to actually start at 0, one needs to supply a null
                // Opinion - this is inconsistent with ReadStream API.
﻿                int? unintuitiveStartPosition = startPosition - 1;
﻿                if(unintuitiveStartPosition < 0)
﻿                {
﻿                    unintuitiveStartPosition = null;
﻿                }
﻿                EventStoreStreamCatchUpSubscription subscription = _connection.SubscribeToStreamFrom(
﻿                    streamId,
                    unintuitiveStartPosition,
﻿                    true,
﻿                    (_, resolvedEvent) => eventAppeared(resolvedEvent),
﻿                    subscriptionDropped: (_, reason, exception) => gesSubscriptionDropped(reason, exception));

                return new StreamCatchupSubscripton(subscription, name);
            }
﻿        }

﻿        public async Task<IAllStreamSubscription> SubscribeToAll(
﻿            string fromCheckpoint,
﻿            StreamEventReceived streamEventReceived,
﻿            SubscriptionDropped subscriptionDropped,
﻿            string name,
﻿            CancellationToken cancellationToken = default(CancellationToken))
﻿        {
            streamEventReceived = FilterOutSystemEvents(streamEventReceived);
            subscriptionDropped = subscriptionDropped ?? ((_, __) => { });
﻿            name = string.IsNullOrWhiteSpace(name) ? Guid.NewGuid().ToString() : name;

            Action<ResolvedEvent> eventAppeared = (resolvedEvent) =>
            {
                var task = Task.Run(async () =>
                {
                    await streamEventReceived(resolvedEvent.ToSteamEvent()).NotOnCapturedContext();
                });

                task.GetAwaiter().GetResult();
            };

            Action<SubscriptionDropReason, Exception> gesSubscriptionDropped =
                (reason, exception) =>
                {
                    subscriptionDropped(reason.ToString(), exception);
                };

            if (fromCheckpoint == null || fromCheckpoint == EndCheckpoint)
            {
                var subscription = await _connection.SubscribeToAllAsync(
                    true,
                    (_, resolvedEvent) => eventAppeared(resolvedEvent),
                    (_, reason, exception) => gesSubscriptionDropped(reason, exception));

                return new AllStreamSubscripton(subscription, name);
            }
            else
            {
               /* // GES catchup subscription starts at supplied startposition + 1;
                // So if you want to actually start at 0, one needs to supply a null
                // Opinion - this is inconsistent with ReadStream API.
                int? unintuitiveStartPosition = startPosition - 1;
                if (unintuitiveStartPosition < 0)
                {
                    unintuitiveStartPosition = null;
                }*/
                var subscription =_connection.SubscribeToAllFrom(
                    PositionCheckpoint.Parse(fromCheckpoint).Position,
                    true,
                    (_, resolvedEvent) => eventAppeared(resolvedEvent),
                    subscriptionDropped: (_, reason, exception) => gesSubscriptionDropped(reason, exception));

                return new AllCatchupSubscripton(subscription, name);
            }
        }

        public void Dispose()
﻿        {
﻿            if(_isDisposed.EnsureCalledOnce())
﻿            {
﻿                return;
﻿            }
﻿            _connection.Dispose();
﻿        }

﻿        private ReadDirection GetReadDirection(global::EventStore.ClientAPI.ReadDirection readDirection)
﻿        {
﻿            return (ReadDirection) Enum.Parse(typeof(ReadDirection), readDirection.ToString());
﻿        }

﻿        private void CheckIfDisposed()
﻿        {
﻿            if(_isDisposed.Value)
﻿            {
﻿                throw new ObjectDisposedException(nameof(GesEventStore));
﻿            }
﻿        }

﻿        private static StreamEventReceived FilterOutSystemEvents(StreamEventReceived streamEventReceived)
﻿        {
            return streamEvent => streamEvent.StreamId.StartsWith("$") ? CompletedTask : streamEventReceived(streamEvent);
        }

﻿        private class StreamSubscripton : IStreamSubscription
﻿        {
﻿            private readonly EventStoreSubscription _eventStoreSubscription;

﻿            internal StreamSubscripton(EventStoreSubscription eventStoreSubscription, string name)
﻿            {
﻿                _eventStoreSubscription = eventStoreSubscription;
﻿                Name = name;
﻿            }

﻿            public void Dispose()
﻿            {
﻿                _eventStoreSubscription.Dispose();
﻿            }

﻿            public string Name { get; }

﻿            public string StreamId => _eventStoreSubscription.StreamId;

﻿            public int LastVersion => _eventStoreSubscription.LastEventNumber.Value;
﻿        }

        private class StreamCatchupSubscripton : IStreamSubscription
        {
            private readonly EventStoreStreamCatchUpSubscription _subscription;

            internal StreamCatchupSubscripton(EventStoreStreamCatchUpSubscription subscription, string name)
            {
                _subscription = subscription;
                Name = name;
            }

            public void Dispose()
            {
                _subscription.Stop();
            }

            public string Name { get; }

            public string StreamId => _subscription.StreamId;

            public int LastVersion => _subscription.LastProcessedEventNumber;
        }

        private class AllStreamSubscripton : IAllStreamSubscription
        {
            private readonly EventStoreSubscription _eventStoreSubscription;

            internal AllStreamSubscripton(EventStoreSubscription eventStoreSubscription, string name)
            {
                _eventStoreSubscription = eventStoreSubscription;
                Name = name;
            }

            public void Dispose()
            {
                _eventStoreSubscription.Dispose();
            }

            public string Name { get; }

            public string LastCheckpoint => _eventStoreSubscription.LastCommitPosition.ToString();
        }

        private class AllCatchupSubscripton : IAllStreamSubscription
        {
            private readonly EventStoreAllCatchUpSubscription _subscription;

            internal AllCatchupSubscripton(EventStoreAllCatchUpSubscription subscription, string name)
            {
                _subscription = subscription;
                Name = name;
            }

            public void Dispose()
            {
                _subscription.Stop();
            }

            public string Name { get; }

            public string LastCheckpoint => _subscription.LastProcessedPosition.ToString();
        }
    }
﻿}