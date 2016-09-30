namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public interface IReadonlyStreamStore : IDisposable
    {
        /// <summary>
        ///     Reads messages from all streams forwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.Start"/> to start from the beginning.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>
        ///     An <see cref="AllMessagesPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<AllMessagesPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Reads messages from all streams backwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.End"/> to start from the end.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="maxCount">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea). 
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="AllMessagesPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<AllMessagesPage> ReadAllBackwards(
            long fromPositionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Reads messages from a stream forwards.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID to read.
        /// </param>
        /// <param name="fromVersionInclusive">
        ///     The version of the stream to start reading from. Use <see cref="StreamVersion.Start"/> to read from 
        ///     the start.
        /// </param>
        /// <param name="maxCount">The maximum number of messages to read (int.MaxValue is a bad idea).</param>
        /// <param name="cancellationToken"> The cancellation instruction. </param>
        /// <returns>
        ///     An <see cref="StreamMessagesPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<StreamMessagesPage> ReadStreamForwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Reads messages from a stream backwards.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID to read.
        /// </param>
        /// <param name="fromVersionInclusive">
        ///     The version of the stream to start reading from. Use <see cref="StreamVersion.End"/> to read from 
        ///     the end.
        /// </param>
        /// <param name="maxCount">T he maximum number of messages to read (int.MaxValue is a bad idea).</param>
        /// <param name="cancellationToken"> The cancellation instruction. </param>
        /// <returns>
        ///     An <see cref="StreamMessagesPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<StreamMessagesPage> ReadStreamBackwards(
            string streamId,
            int fromVersionInclusive,
            int maxCount,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Subsribes to a stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to subscribe to.
        /// </param>
        /// <param name="fromVersionExclusive">
        ///     The version to subscribe from.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription fails.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IStreamSubscription SubscribeToStream(
            string streamId,
            int fromVersionExclusive,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null);

        /// <summary>
        ///     Subsribes to the all stream.
        /// </summary>
        /// <param name="continueAfterPosition">
        ///     The position from which the subscription will continue after.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription fails.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IAllStreamSubscription SubscribeToAll(
            long continueAfterPosition,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null);

        /// <summary>
        ///     Subsribes to the all stream.
        /// </summary>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription fails.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IAllStreamSubscription SubscribeToAllFromStart(
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            string name = null);

        /// <summary>
        ///     Reads the head position (the postion of the very latest message).
        /// </summary>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     The head positon.
        /// </returns>
        Task<long> ReadHeadPosition(CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Gets the stream metadata.
        /// </summary>
        /// <param name="streamId">
        ///     The stream ID whose metadata is to be read.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     A <see cref="StreamMetadataResult"/> 
        /// </returns>
        Task<StreamMetadataResult> GetStreamMetadata(
            string streamId,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}