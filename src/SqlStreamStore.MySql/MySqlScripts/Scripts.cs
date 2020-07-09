namespace SqlStreamStore.MySqlScripts
{
    using System;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Reflection;

    internal class Scripts
    {
        private static readonly Assembly s_assembly = typeof(Scripts)
            .GetTypeInfo()
            .Assembly;

        private readonly ConcurrentDictionary<string, string> _scripts
            = new ConcurrentDictionary<string, string>();

        public string DropAll => GetScript(nameof(DropAll));

        private string Tables => GetScript(nameof(Tables));

        private string AppendToStreamExpectedVersion => GetScript(nameof(AppendToStreamExpectedVersion));

        private string AppendToStreamExpectedVersionAny => GetScript(nameof(AppendToStreamExpectedVersionAny));

        private string AppendToStreamExpectedVersionEmptyStream =>
            GetScript(nameof(AppendToStreamExpectedVersionEmptyStream));

        private string AppendToStreamExpectedVersionNoStream =>
            GetScript(nameof(AppendToStreamExpectedVersionNoStream));

        private string CreateEmptyStream => GetScript(nameof(CreateEmptyStream));

        private string DeleteStream => GetScript(nameof(DeleteStream));

        private string DeleteStreamMessage => GetScript(nameof(DeleteStreamMessage));

        private string GetStreamMetadata => GetScript(nameof(GetStreamMetadata));

        private string ListStreams => GetScript(nameof(ListStreams));

        private string ListStreamsStartingWith => GetScript(nameof(ListStreamsStartingWith));

        private string ListStreamsEndingWith => GetScript(nameof(ListStreamsEndingWith));

        private string ReadObsolete => GetScript(nameof(ReadObsolete));

        private string ReadStreamForwards => GetScript(nameof(ReadStreamForwards));

        private string ReadStreamForwardsWithData => GetScript(nameof(ReadStreamForwardsWithData));

        private string ReadStreamBackwards => GetScript(nameof(ReadStreamBackwards));

        private string ReadStreamBackwardsWithData => GetScript(nameof(ReadStreamBackwardsWithData));

        private string ReadAllObsolete => GetScript(nameof(ReadAllObsolete));

        private string ReadAllBackwards => GetScript(nameof(ReadAllBackwards));

        private string ReadAllBackwardsWithData => GetScript(nameof(ReadAllBackwardsWithData));

        private string ReadAllForwards => GetScript(nameof(ReadAllForwards));

        private string ReadAllForwardsWithData => GetScript(nameof(ReadAllForwardsWithData));


        private string ReadJsonData => GetScript(nameof(ReadJsonData));

        private string ReadHeadPosition => GetScript(nameof(ReadHeadPosition));

        private string ReadStreamHeadPosition => GetScript(nameof(ReadStreamHeadPosition));

        private string ReadStreamHeadVersion => GetScript(nameof(ReadStreamHeadVersion));

        private string ReadProperties => GetScript(nameof(ReadProperties));

        private string ReadStreamVersionOfMessageId => GetScript(nameof(ReadStreamVersionOfMessageId));

        private string Scavenge => GetScript(nameof(Scavenge));

        private string SetStreamMetadata => GetScript(nameof(SetStreamMetadata));


        public string CreateSchema => string.Join(
            Environment.NewLine,
            Tables,
            AppendToStreamExpectedVersion,
            AppendToStreamExpectedVersionAny,
            AppendToStreamExpectedVersionEmptyStream,
            AppendToStreamExpectedVersionNoStream,
            CreateEmptyStream,
            DeleteStream,
            DeleteStreamMessage,
            GetStreamMetadata,
            ListStreams,
            ListStreamsStartingWith,
            ListStreamsEndingWith,
            ReadObsolete,
            ReadStreamForwards,
            ReadStreamForwardsWithData,
            ReadStreamBackwards,
            ReadStreamBackwardsWithData,
            ReadAllObsolete,
            ReadAllBackwards,
            ReadAllBackwardsWithData,
            ReadAllForwards,
            ReadAllForwardsWithData,
            ReadJsonData,
            ReadHeadPosition,
            ReadStreamHeadPosition,
            ReadStreamHeadVersion,
            ReadProperties,
            ReadStreamVersionOfMessageId,
            Scavenge,
            SetStreamMetadata);

        private string GetScript(string name) => _scripts.GetOrAdd(name,
            key =>
            {
                using(var stream = s_assembly.GetManifestResourceStream(typeof(Scripts), $"{key}.sql"))
                {
                    if(stream == null)
                    {
                        throw new Exception($"Embedded resource, {name}, not found. BUG!");
                    }

                    using(StreamReader reader = new StreamReader(stream))
                    {
                        return reader.ReadToEnd();
                    }
                }
            });
    }
}