namespace Cedar.EventStore.Scavenging
{
    using System;
    using Xunit;

    public class ScavengerTests
    {
        [Fact]
        public void Blah()
        {
            using(var eventStore = new InMemoryEventStore())
            {
                using(var scavenger = new Scavenger(eventStore, $"scavenger/{Guid.NewGuid()}"))
                {
                    scavenger.Blah();
                }
            }
        }
    }
}
