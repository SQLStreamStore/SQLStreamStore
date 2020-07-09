namespace SqlStreamStore.ScriptsV1
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

    public class Scripts
    {
        private readonly ConcurrentDictionary<string, string> _scripts 
            = new ConcurrentDictionary<string, string>();

        internal string AppendStreamExpectedVersionAny => GetScript(nameof(AppendStreamExpectedVersionAny));

        internal string AppendStreamExpectedVersion => GetScript(nameof(AppendStreamExpectedVersion));

        internal string AppendStreamExpectedVersionNoStream => GetScript(nameof(AppendStreamExpectedVersionNoStream));

        internal string DeleteStreamAnyVersion => GetScript(nameof(DeleteStreamAnyVersion));

        internal string DeleteStreamMessage => GetScript(nameof(DeleteStreamMessage));

        internal string DeleteStreamExpectedVersion => GetScript(nameof(DeleteStreamExpectedVersion));

        internal string DropAll => GetScript(nameof(DropAll));

        internal string GetStreamMessageCount => GetScript(nameof(GetStreamMessageCount));

        internal string CreateSchema => GetScript(nameof(CreateSchema));

        internal string GetSchemaVersion => GetScript(nameof(GetSchemaVersion));

        internal string GetStreamVersionOfMessageId => GetScript(nameof(GetStreamVersionOfMessageId));

        internal string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));

        internal string ReadStreamHeadPosition => GetScript(nameof(ReadStreamHeadPosition));

        internal string ReadStreamHeadVersion => GetScript(nameof(ReadStreamHeadVersion));

        public string ReadAllForward => GetScript(nameof(ReadAllForward));

        internal string ReadAllForwardWithData => GetScript(nameof(ReadAllForwardWithData));

        internal string ReadAllBackward => GetScript(nameof(ReadAllBackward));

        internal string ReadAllBackwardWithData => GetScript(nameof(ReadAllBackwardWithData));

        internal string ReadStreamForward => GetScript(nameof(ReadStreamForward));

        internal string ReadStreamForwardWithData => GetScript(nameof(ReadStreamForwardWithData));

        internal string ReadStreamBackward => GetScript(nameof(ReadStreamBackward));

        internal string ReadStreamBackwardWithData => GetScript(nameof(ReadStreamBackwardWithData));

        internal string ReadMessageData => GetScript(nameof(ReadMessageData));

        internal string SetStreamMetadata => GetScript(nameof(SetStreamMetadata));

        internal string ListStreamIds => GetScript(nameof(ListStreamIds));

        internal string ListStreamIdsStartingWith => GetScript(nameof(ListStreamIdsStartingWith));

        internal string ListStreamIdsEndingWith => GetScript(nameof(ListStreamIdsEndingWith));

        private string GetScript(string name) =>
            _scripts.GetOrAdd(name, ScriptValueFactory(name));

        private static Func<string, string> ScriptValueFactory(string name) =>
            key =>
            {
                using (Stream stream = typeof(Scripts).GetTypeInfo().Assembly.GetManifestResourceStream("SqlStreamStore.ScriptsV1." + key + ".sql"))
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
            };
    }
}