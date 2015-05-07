namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    ///     Represents metadata associated withan event. Keys are compared with <see cref="StringComparer.OrdinalIgnoreCase" />
    /// </summary>
    public class Metadata
    {
        private readonly Dictionary<string, string> _metadataJson
            = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        private readonly IJsonSerializer _serializer;

        public Metadata(IJsonSerializer serializer = null)
        {
            _serializer = serializer ?? DefaultJsonSerializer.Instance;
        }

        public object this[string key]
        {
            set { _metadataJson[key] = _serializer.Serialize(value); }
        }

        public T Get<T>(string key)
        {
            if(!_metadataJson.ContainsKey(key))
            {
                throw new KeyNotFoundException("The given key was not present in the metadata.");
            }
            return _serializer.Deserialize<T>(_metadataJson[key]);
        }
    }
}