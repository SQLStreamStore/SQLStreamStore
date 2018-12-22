namespace SqlStreamStore.Infrastructure
{
    using System;

    /// <summary>
    ///     A deterministic GUID generator for metadata messages.
    /// </summary>
    public static class MetadataMessageIdGenerator
    {
        private static readonly DeterministicGuidGenerator s_deterministicGuidGenerator;

        static MetadataMessageIdGenerator()
        {
            s_deterministicGuidGenerator 
                = new DeterministicGuidGenerator(Guid.Parse("8D1E0B02-0D78-408E-8211-F899BE6F8AA2"));
        }

        /// <summary>
        ///     Create a GUID for metadata message Ids.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID.
        /// </param>
        /// <param name="expectedVersion">
        ///     The expected version of the stream the message will be appended to.
        /// </param>
        /// <param name="message">
        ///     The metadata message uses as input into the generation algorithm.
        /// </param>
        /// <returns>
        ///     A deterministically generated GUID.
        /// </returns>
        public static Guid Create(string streamId, int expectedVersion, string message)
        {
            return s_deterministicGuidGenerator.Create(streamId, expectedVersion, message);
        }
    }
}