namespace SqlStreamStore
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using SqlStreamStore.Streams;

    public partial class SqliteStreamStore
    {
        protected override async Task<AppendResult> AppendToStreamInternal(
            string streamId,
            int expectedVersion,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            GuardAgainstDisposed();
            cancellationToken.ThrowIfCancellationRequested();

            SqliteAppendResult result;

            switch(expectedVersion)
            {
                case ExpectedVersion.Any:
                    result = await AppendToStreamAnyVersion(streamId, messages, cancellationToken).ConfigureAwait(false);
                    break;
                case ExpectedVersion.EmptyStream:
                    result = await AppendToStreamEmpty(streamId, messages, cancellationToken).ConfigureAwait(false);
                    break;
                case ExpectedVersion.NoStream:
                    result = await AppendToNonexistentStream(streamId, messages, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    result = AppendToStreamExpectedVersion(streamId, expectedVersion, messages, cancellationToken);
                    break;
            }

            if(result.MaxCount.HasValue)
            {
                await CheckStreamMaxCount(streamId, result.MaxCount, cancellationToken).ConfigureAwait(false);
            }

            await TryScavengeAsync(streamId, cancellationToken);

            return result;
        }

        private Task<SqliteAppendResult> AppendToStreamAnyVersion(
            string streamId,
            NewStreamMessage[] messages,
            CancellationToken cancellationToken)
        {
            using(var connection = OpenConnection(false))
            {
                var stream = connection.Streams(streamId);
                var allStream = connection.AllStream();

                var props = stream.Properties(true, cancellationToken)
                    .GetAwaiter().GetResult();

                if(messages.Length == 1)
                {
                    var msg = messages[0];

                    var exists = stream.Contains(msg.MessageId)
                        .GetAwaiter().GetResult();

                    if(exists)
                    {
                        return Task.FromResult(new SqliteAppendResult(props.Version, props.Position, null));
                    }
                }
                else if(messages.Any())
                {
                    var msg1 = messages.First();
                    var position = stream.AllStreamPosition(ReadDirection.Forward, msg1.MessageId)
                        .GetAwaiter().GetResult() ?? long.MaxValue;

                    var sMessages = stream.Read(ReadDirection.Forward, position, false, messages.Length)
                        .GetAwaiter()
                        .GetResult();
                    var eventIds = sMessages.Select(m => m.MessageId).ToArray();

                    if(eventIds.Length > 0)
                    {
                        for(var i = 0; i < Math.Min(eventIds.Length, messages.Length); i++)
                        {
                            if(eventIds[i] != messages[i].MessageId)
                            {
                                throw new WrongExpectedVersionException(
                                    ErrorMessages.AppendFailedWrongExpectedVersion(
                                        streamId,
                                        StreamVersion.Start),
                                    streamId,
                                    StreamVersion.Start);
                            }
                        }

                        if(eventIds.Length < messages.Length && eventIds.Length > 0)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId,
                                    StreamVersion.Start),
                                streamId,
                                StreamVersion.Start);
                        }

                        return Task.FromResult(new SqliteAppendResult(props.Version, props.Position, null));
                    }
                }

                using(allStream.WithTransaction())
                {
                    var result = allStream.Append(streamId, messages)
                        .GetAwaiter().GetResult();

                    allStream.Commit(cancellationToken)
                        .GetAwaiter().GetResult();

                    return Task.FromResult(result);
                }
            }
        }

        private async Task<SqliteAppendResult> AppendToStreamEmpty(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            using(var conn = OpenConnection(false))
            {
                var stream = conn.Streams(streamId);
                var allStream = conn.AllStream();

                var length = await stream.Length(cancellationToken);

                if(length > StreamVersion.Start)
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId,
                            StreamVersion.Start),
                        streamId,
                        StreamVersion.Start);
                }

                using(allStream.WithTransaction())
                {
                    var result = await allStream.Append(streamId, messages);
                    await allStream.Commit(cancellationToken);
                    return result;
                }
            }
        }

        private async Task<SqliteAppendResult> AppendToNonexistentStream(string streamId, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            using(var connection = OpenConnection(false))
            {
                var stream = connection.Streams(streamId);

                if(await stream.Exists())
                {
                    var position = await stream.AllStreamPosition(ReadDirection.Forward, StreamVersion.Start);
                    var eventIds = (await stream.Read(ReadDirection.Forward, position, false, int.MaxValue - 1))
                        .Select(message => message.MessageId)
                        .ToArray();

                    if(eventIds.Length > 0)
                    {
                        for(var i = 0; i < Math.Min(eventIds.Length, messages.Length); i++)
                        {
                            if(eventIds[i] != messages[i].MessageId)
                            {
                                throw new WrongExpectedVersionException(
                                    ErrorMessages.AppendFailedWrongExpectedVersion(
                                        streamId,
                                        ExpectedVersion.NoStream),
                                    streamId,
                                    ExpectedVersion.NoStream);
                            }
                        }

                        if(eventIds.Length < messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId,
                                    ExpectedVersion.NoStream),
                                streamId,
                                ExpectedVersion.NoStream);
                        }

                        var header = stream.Properties(false, cancellationToken)
                            .GetAwaiter().GetResult();

                        return new SqliteAppendResult(header.Version, header.Position, null);
                    }
                }

                using(stream.WithTransaction())
                {
                    // this will create the stream information so messages can be appended.
                    stream.Properties(true, cancellationToken)
                        .GetAwaiter().GetResult();

                    await stream.Commit(cancellationToken);
                }
            }

            return await AppendToStreamEmpty(streamId, messages, cancellationToken);
        }

        private SqliteAppendResult AppendToStreamExpectedVersion(string streamId, int expectedVersion, NewStreamMessage[] messages, CancellationToken cancellationToken)
        {
            using(var connection = OpenConnection(false))
            {
                var stream = connection.Streams(streamId);

                if(!stream.Exists().GetAwaiter().GetResult())
                {
                    throw new WrongExpectedVersionException(
                        ErrorMessages.AppendFailedWrongExpectedVersion(
                            streamId,
                            expectedVersion),
                        streamId,
                        expectedVersion);
                }

                var props = connection.Streams(streamId)
                    .Properties(initializeIfNotFound: false, cancellationToken)
                    .GetAwaiter().GetResult();

                if(messages.Length == 1)
                {
                    var msg = messages.First();

                    // tries to fix "When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result"
                    if(stream.ExistsAtExpectedPosition(msg.MessageId, expectedVersion).GetAwaiter().GetResult())
                    {
                        return new SqliteAppendResult(
                            props.Version,
                            props.Position,
                            null
                        );
                    }
                    // end - tries to fix "When_append_single_message_to_stream_with_correct_expected_version_second_time_with_same_initial_messages_then_should_have_expected_result"


                    // tries to fix "When_append_stream_with_expected_version_and_duplicate_message_Id_then_should_throw"
                    if(stream.Exists(msg.MessageId, expectedVersion).GetAwaiter().GetResult())
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamId,
                                expectedVersion),
                            streamId,
                            expectedVersion);
                    }
                    // end - tries to fix "When_append_stream_with_expected_version_and_duplicate_message_Id_then_should_throw"

                    var eventIds = stream.Read(ReadDirection.Forward, expectedVersion, false, int.MaxValue)
                        .GetAwaiter().GetResult()
                        .Select(message => message.MessageId)
                        .ToArray();

                    if(eventIds.Contains(msg.MessageId))
                    {
                        if(eventIds.Length > messages.Length)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId,
                                    ExpectedVersion.NoStream),
                                streamId,
                                ExpectedVersion.NoStream);
                        }
                        return new SqliteAppendResult(
                            props.Version,
                            props.Position,
                            null
                        );
                    }
                }

                if(expectedVersion != props.Version)
                {
                    var msg = messages.First();

                    var position = stream.AllStreamPosition(ReadDirection.Forward, msg.MessageId)
                        .GetAwaiter().GetResult();

                    // retrieve next series of messages from the first message being requested to
                    var eventIds = position.HasValue
                        ? stream.Read(ReadDirection.Forward, position, false, messages.Length)
                        .GetAwaiter().GetResult()
                        .Select(message => message.MessageId)
                        .ToArray()
                        : new Guid[0];

                    if(messages.Length != eventIds.Length)
                    {
                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(
                                streamId,
                                expectedVersion),
                            streamId,
                            expectedVersion);
                    }

                    // tests for positional inequality between what we know and what is stored.
                    for(var i = 0; i < Math.Min(messages.Length, eventIds.Length); i++)
                    {
                        var nextMessageId = eventIds.Skip(i).Take(1).SingleOrDefault();
                        if(messages[i].MessageId != nextMessageId)
                        {
                            throw new WrongExpectedVersionException(
                                ErrorMessages.AppendFailedWrongExpectedVersion(
                                    streamId,
                                    expectedVersion),
                                streamId,
                                expectedVersion);
                        }
                    }

                    // we seem to be equal.
                    return new SqliteAppendResult(
                        props.Version,
                        props.Position,
                        props.MaxCount
                    );
                }

                var allStream = connection.AllStream();

                using(allStream.WithTransaction())
                {
                    var result = allStream.Append(streamId, messages)
                        .GetAwaiter().GetResult();

                    allStream.Commit(cancellationToken)
                        .GetAwaiter().GetResult();

                    return result;
                }
            }
        }

        private async Task CheckStreamMaxCount(
            string streamId,
            int? maxCount,
            CancellationToken cancellationToken)
        {
            var count = await OpenConnection()
                .Streams(streamId)
                .Length(cancellationToken);

            if (count > maxCount)
            {
                int toPurge = count - maxCount.Value;

                var streamMessagesPage = await ReadStreamForwardsInternal(streamId, StreamVersion.Start, toPurge, false, null, cancellationToken)
                    .ConfigureAwait(false);

                if (streamMessagesPage.Status == PageReadStatus.Success)
                {
                    foreach (var message in streamMessagesPage.Messages)
                    {
                        await DeleteEventInternal(streamId, message.MessageId, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }
    }
}
