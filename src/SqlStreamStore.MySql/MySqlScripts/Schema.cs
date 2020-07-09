namespace SqlStreamStore.MySqlScripts
{
    internal class Schema
    {
        private readonly Scripts _scripts;

        public string Definition => _scripts.CreateSchema;

        public string DropAll => _scripts.DropAll;

        public string AppendToStreamExpectedVersion => "append_to_stream_expected_version";

        public string AppendToStreamExpectedVersionAny => "append_to_stream_expected_version_any";

        public string AppendToStreamExpectedVersionNoStream => "append_to_stream_expected_version_no_stream";

        public string AppendToStreamExpectedVersionEmptyStream => "append_to_stream_expected_version_empty_stream";

        public string CreateEmptyStream => "create_empty_stream";

        public string Scavenge => "scavenge";

        public string SetStreamMetadata => "set_stream_metadata";

        public string DeleteStream => "delete_stream";

        public string DeleteStreamMessage => "delete_stream_message";

        public string ListStreams => "list_streams";

        public string ListStreamsStartingWith => "list_streams_starting_with";

        public string ListStreamsEndingWith => "list_streams_ending_with";

        public string Read => "`read`";

        public string ReadStreamForwards => "`read_stream_forwards`";

        public string ReadStreamForwardsWithData => "`read_stream_forwards_with_data`";

        public string ReadStreamBackwards => "`read_stream_backwards`";

        public string ReadStreamBackwardsWithData => "`read_stream_backwards_with_data`";

        public string ReadAllBackwards => "read_all_backwards";

        public string ReadAllBackwardsWithData => "read_all_backwards_with_data";

        public string ReadAllForwards => "read_all_forwards";

        public string ReadAllForwardsWithData => "read_all_forwards_with_data";

        public string ReadAllHeadPosition => "read_head_position";

        public string ReadStreamHeadPosition => "read_stream_head_position";

        public string ReadStreamHeadVersion => "read_stream_head_version";

        public string ReadJsonData => "read_json_data";

        public string ReadProperties => "read_properties";

        public Schema()
        {
            _scripts = new Scripts();
        }
    }
}