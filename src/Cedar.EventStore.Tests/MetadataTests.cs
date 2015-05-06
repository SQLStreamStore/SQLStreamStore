namespace Cedar.EventStore
{
    using System;
    using System.Collections.Generic;
    using FluentAssertions;
    using Xunit;

    public class MetadataTests
    {
        [Fact]
        public void Can_set_and_get_metadata()
        {
            var metadata = new Metadata();

            metadata["key"] = "value";

            metadata.Get<string>("key").Should().Be("value");
        }

        [Fact]
        public void When_key_does_not_exist_and_get_then_should_throw()
        {
            var metadata = new Metadata();

            Action act = () => metadata.Get<string>("key");

            act.ShouldThrow<KeyNotFoundException>();
        }
    }

    /// <summary>
    /// Represents metadata associated withan event. Keys are compared with <see cref="StringComparer.OrdinalIgnoreCase"/>
    /// </summary>
    public class Metadata
    {
        private readonly Dictionary<string, string> _metadataJson
            = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private readonly ISerializer _serializer;

        public Metadata(ISerializer serializer = null)
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