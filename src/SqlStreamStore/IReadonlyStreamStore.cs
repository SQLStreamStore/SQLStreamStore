﻿namespace SqlStreamStore
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IReadonlyStreamStore : IDisposable
    {
        /// <summary>
        ///     Reads messages from all streams forwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.Start"/> to start from the beginning.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="pageSize">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadAllPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<ReadAllPage> ReadAllForwards(
            long fromPositionInclusive,
            int pageSize,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Reads messages from all streams backwards.
        /// </summary>
        /// <param name="fromPositionInclusive">
        ///     The position to start reading from. Use <see cref="Position.End"/> to start from the end.
        ///     Note: messages that have expired will be filtered out.
        /// </param>
        /// <param name="pageSize">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadAllPage"/> presenting the result of the read. If all messages read have expired
        ///     then the message collection MAY be empty.
        /// </returns>
        Task<ReadAllPage> ReadAllBackwards(
            long fromPositionInclusive,
            int pageSize,
            bool prefetchJsonData = true,
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
        /// <param name="pageSize">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadStreamPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<ReadStreamPage> ReadStreamForwards(
            StreamId streamId,
            int fromVersionInclusive,
            int pageSize,
            bool prefetchJsonData = true,
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
        /// <param name="pageSize">
        ///     The maximum number of messages to read (int.MaxValue is a bad idea).
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="cancellationToken">
        ///     The cancellation instruction.
        /// </param>
        /// <returns>
        ///     An <see cref="ReadStreamPage"/> represent the result of the operation. If all the messages read
        ///     have expired then the message collection MAY be empty.
        /// </returns>
        Task<ReadStreamPage> ReadStreamBackwards(
            StreamId streamId,
            int fromVersionInclusive,
            int pageSize,
            bool prefetchJsonData = true,
            CancellationToken cancellationToken = default(CancellationToken));

        /// <summary>
        ///     Subsribes to a stream.
        /// </summary>
        /// <param name="streamId">
        ///     The stream to subscribe to.
        /// </param>
        /// <param name="continueAfterVersion">
        ///     The version to subscribe from.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription is dropped. This will be invoked once and only once.
        /// </param>
        /// <param name="hasCaughtUp">
        ///     A delegate that is invoked with value=true when the subscription has caught up with the stream
        ///     (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read
        ///     has IsEnd=false). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IStreamSubscription SubscribeToStream(
            StreamId streamId,
            int? continueAfterVersion,
            StreamMessageReceived streamMessageReceived,
            SubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
            string name = null);

        /// <summary>
        ///     Subsribes to all stream.
        /// </summary>
        /// <param name="continueAfterPosition">
        ///     The position to start subscribing after. Use null to include the first message.
        /// </param>
        /// <param name="streamMessageReceived">
        ///     A delegate that is invoked when a message is available. If an exception is thrown, the subscription
        ///     is terminated.
        /// </param>
        /// <param name="subscriptionDropped">
        ///     A delegate that is invoked when a the subscription is dropped. This will be invoked once and only once.
        /// </param>
        /// <param name="hasCaughtUp">
        ///     A delegate that is invoked with value=true when the subscription has catught up with the all stream
        ///     (when the underlying page read has IsEnd=true) and when it falls behind (when the underlying page read
        ///     has IsEnd=false). 
        /// </param>
        /// <param name="prefetchJsonData">
        ///     Prefetches the message data as part of the page read. This means a single request to the server
        ///     but a higher payload size.
        /// </param>
        /// <param name="name">
        ///     The name of the subscription used for logging. Optional.
        /// </param>
        /// <returns>
        ///     An <see cref="IStreamSubscription"/> that represents the subscription. Dispose to stop the subscription.
        /// </returns>
        IAllStreamSubscription SubscribeToAll(
            long? continueAfterPosition,
            AllStreamMessageReceived streamMessageReceived,
            AllSubscriptionDropped subscriptionDropped = null,
            HasCaughtUp hasCaughtUp = null,
            bool prefetchJsonData = true,
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

        event Action OnDispose;
    }
}