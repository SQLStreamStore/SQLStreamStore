namespace SqlStreamStore.Infrastructure
{
    using System;
    using System.Linq;
    using System.Text;

    public static class DeterministicGuidGeneratorExtensions
    {
        /// <summary>
        ///     Creates a deterministic GUID.
        /// </summary>
        /// <param name="generator">
        ///     A <see cref="DeterministicGuidGenerator"/> instance.
        /// </param>
        /// <param name="streamId">
        ///     The Stream ID the message is being appended to.
        /// </param>
        /// <param name="expectedVersion">
        ///     The expected version of the message.
        /// </param>
        /// <param name="message">
        ///     A message to generate the GUID from.
        /// </param>
        /// <returns>
        ///     A deterministically generated GUID.
        /// </returns>
        public static Guid Create(
            this DeterministicGuidGenerator generator,
            string streamId,
            int expectedVersion, 
            string message)
            => generator.Create(
                Encoding.UTF8.GetBytes(message)
                    .Concat(BitConverter.GetBytes(expectedVersion))
                    .Concat(Encoding.UTF8.GetBytes(streamId)).ToArray());
    }
}