namespace Cedar.EventStore
{
    using System;
    using SQLite.Net.Attributes;

    [Table("Events")]
    internal class SqliteEvent
    {
        [MaxLength(40), NotNull]
        public string StreamId { get; set; }

        [NotNull]
        public Guid EventId { get; set; }

        public int SequenceNumber { get; set; }

        [PrimaryKey, AutoIncrement]
        public int Checkpoint { get; set; }

        [NotNull]
        public string OriginalStreamId { get; set; }

        [NotNull]
        public bool IsDeleted { get; set; }

        public string Type { get; set; }

        public string JsonMetadata { get; set; }

        [NotNull]
        public string JsonData { get; set; }

        [NotNull]
        public DateTime Created { get; set; }
    }
}