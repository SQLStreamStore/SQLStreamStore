namespace SqlStreamStore.SQLiteScripts
{
    internal class Schema
    {
        private readonly string _schema;
        private readonly Scripts _scripts;

        public string Definition => _scripts.CreateSchema;

        public Schema(string schema)
        {
            _schema = schema;
            _scripts = new Scripts(schema);
        }
    }
}