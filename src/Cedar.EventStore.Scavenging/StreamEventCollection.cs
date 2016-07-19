namespace Cedar.EventStore.Scavenging
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;

    internal class StreamEventCollection
    {
        private readonly Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>> _streamEventsByEventId
            = new Dictionary<string, Dictionary<Guid, ScavengerStreamEvent>>();
        private readonly Dictionary<string, LinkedList<ScavengerStreamEvent>> _streamEvents
            = new Dictionary<string, LinkedList<ScavengerStreamEvent>>();
        private readonly Dictionary<string, ScavengerStreamMetadata> _streamMetadata
            = new Dictionary<string, ScavengerStreamMetadata>();

        internal void Add(ScavengerStreamEvent streamEvent)
        {
            var streamId = streamEvent.StreamId;
            var eventId = streamEvent.EventId;

            if (!_streamEventsByEventId.ContainsKey(streamId))
            {
                _streamEventsByEventId.Add(streamId, new Dictionary<Guid, ScavengerStreamEvent>());
            }

            if (!_streamEvents.ContainsKey(streamId))
            {
                _streamEvents.Add(streamId, new LinkedList<ScavengerStreamEvent>());
            }

            // idempotent handling. If we've already seen this event, then skip processing
            if (!_streamEventsByEventId[streamId].ContainsKey(eventId)) 
            {
                var scavengerStreamEvent = new ScavengerStreamEvent(
                    streamId, eventId, streamEvent.Created);

                // if the stream has metadata associated with it, adjust the expiry accordingly.
                ScavengerStreamMetadata metadata;
                if (_streamMetadata.TryGetValue($"{streamId}", out metadata))
                {
                    if (metadata.MaxAge.HasValue)
                    {
                        scavengerStreamEvent.SetExpires(metadata.MaxAge.Value);
                    }
                }

                _streamEventsByEventId[streamId].Add(streamEvent.EventId, scavengerStreamEvent);
                _streamEvents[streamId].AddLast(scavengerStreamEvent);
            }
        }

        internal IReadOnlyCollection<string> GetStreams()
        {
            return new ReadOnlyCollection<string>(_streamEventsByEventId.Keys.ToList());
        }

        internal IReadOnlyDictionary<Guid, ScavengerStreamEvent> GetEvents(string streamId)
        {
            return new ReadOnlyDictionary<Guid, ScavengerStreamEvent>(_streamEventsByEventId[streamId]);
        }

        internal void AddOrUpdate(ScavengerStreamMetadata metadata)
        {
            if (!_streamMetadata.ContainsKey(metadata.StreamId))
            {
                _streamMetadata.Add(metadata.StreamId, metadata);
            }
            else
            {
                _streamMetadata[metadata.StreamId] = metadata;
            }

            // Reset the expiry stamps for events affected by the metadata change.
            Dictionary<Guid, ScavengerStreamEvent> scavengerStreamEvents;
            if (_streamEventsByEventId.TryGetValue(metadata.StreamId, out scavengerStreamEvents))
            {
                foreach (var value in scavengerStreamEvents.Values)
                {
                    value.SetExpires(metadata.MaxAge);
                }
            }
        }

        internal ScavengerStreamEvent[] GetStream(string streamId)
        {
            if (!_streamEventsByEventId.ContainsKey(streamId))
            {
                return null;
            }
            return _streamEventsByEventId[streamId].Values.ToArray();
        }

        internal ScavengerStreamEvent GetStreamEvent(string streamId, Guid eventId)
        {
            if (!_streamEventsByEventId.ContainsKey(streamId))
            {
                return null;
            }
            if (!_streamEventsByEventId[streamId].ContainsKey(eventId))
            {
                return null;
            }
            return _streamEventsByEventId[streamId][eventId];
        }

        internal void RemoveEvent(string streamId, Guid eventId)
        {
            if(_streamEventsByEventId.ContainsKey(streamId))
            {
                if (_streamEventsByEventId[streamId].ContainsKey(eventId))
                {
                    var scavengerStreamEvent = _streamEventsByEventId[streamId][eventId];
                    _streamEventsByEventId[streamId].Remove(eventId);

                    _streamEvents[streamId].Remove(scavengerStreamEvent);
                }
            }
        }

        public void RemoveStream(string streamId)
        {
            if(_streamEventsByEventId.ContainsKey(streamId))
            {
                _streamEventsByEventId.Remove(streamId);
                _streamEvents.Remove(streamId);
            }
        }
    }
}