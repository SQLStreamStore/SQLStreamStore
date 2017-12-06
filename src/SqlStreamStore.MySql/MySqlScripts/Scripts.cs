namespace SqlStreamStore.MySqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

    internal class Scripts
    {
        private readonly ConcurrentDictionary<string, string> _scripts 
            = new ConcurrentDictionary<string, string>();

        internal string AppendStreamExpectedVersionAny => GetScript(nameof(AppendStreamExpectedVersionAny));

        internal string AppendStreamExpectedVersion => GetScript(nameof(AppendStreamExpectedVersion));

        internal string AppendStreamExpectedVersionNoStream => GetScript(nameof(AppendStreamExpectedVersionNoStream));

        internal string AppendStreamExpectedVersionAnyEmpty => GetScript(nameof(AppendStreamExpectedVersionAnyEmpty));

        internal string AppendStreamExpectedVersionEmpty => GetScript(nameof(AppendStreamExpectedVersionEmpty));

        internal string AppendStreamExpectedVersionNoStreamEmpty => GetScript(nameof(AppendStreamExpectedVersionNoStreamEmpty));

        internal string CreateDatabase => GetScript(nameof(CreateDatabase));

        internal string DeleteStreamAnyVersion => GetScript(nameof(DeleteStreamAnyVersion));

        internal string DeleteStreamMessage => GetScript(nameof(DeleteStreamMessage));

        internal string DeleteStreamExpectedVersion => GetScript(nameof(DeleteStreamExpectedVersion));

        internal string DropAll => GetScript(nameof(DropAll));

        internal string GetStreamIdInternal => GetScript(nameof(GetStreamIdInternal));

        internal string GetStreamMessageCount => GetScript(nameof(GetStreamMessageCount));

        internal string GetStreamMessageBeforeCreatedCount => GetScript(nameof(GetStreamMessageBeforeCreatedCount));

        internal string GetSchemaVersion => GetScript(nameof(GetSchemaVersion));

        internal string GetLatestStreamVersion => GetScript(nameof(GetLatestStreamVersion));

        internal string IndexExists => GetScript(nameof(IndexExists));

        internal string IndexMessagesByPosition => GetScript(nameof(IndexMessagesByPosition));

        internal string IndexMessagesByStreamIdInternalAndCreated => GetScript(nameof(IndexMessagesByStreamIdInternalAndCreated));

        internal string IndexMessagesByStreamIdInternalAndId => GetScript(nameof(IndexMessagesByStreamIdInternalAndId));

        internal string IndexMessagesByStreamIdInternalAndStreamVersion=> GetScript(nameof(IndexMessagesByStreamIdInternalAndStreamVersion));

        internal string IndexStreamsById => GetScript(nameof(IndexStreamsById));

        internal string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));

        internal string ReadAllForward => GetScript(nameof(ReadAllForward));

        internal string ReadAllForwardWithData => GetScript(nameof(ReadAllForwardWithData));

        internal string ReadAllBackward => GetScript(nameof(ReadAllBackward));

        internal string ReadAllBackwardWithData => GetScript(nameof(ReadAllBackwardWithData));

        internal string ReadStreamForward => GetScript(nameof(ReadStreamForward));

        internal string ReadStreamForwardWithData => GetScript(nameof(ReadStreamForwardWithData));

        internal string ReadStreamBackward => GetScript(nameof(ReadStreamBackward));

        internal string ReadStreamBackwardWithData => GetScript(nameof(ReadStreamBackwardWithData));

        internal string ReadMessageData => GetScript(nameof(ReadMessageData));

        private string GetScript(string name)
        {
            return _scripts.GetOrAdd(name,
                key =>
                {
                    using (Stream stream = typeof(Scripts).GetTypeInfo().Assembly.GetManifestResourceStream("SqlStreamStore.MySqlScripts." + key + ".sql"))
                    {
                        if (stream == null)
                        {
                            throw new Exception($"Embedded resource, {name}, not found. BUG!");
                        }
                        using (StreamReader reader = new StreamReader(stream))
                        {
                            return reader
                                .ReadToEnd();
                        }
                    }
                });
        }
    }
}