namespace SqlStreamStore.V1
{
    public class MigrateProgress
    {
        public enum MigrateStage
        {
            /// <summary>
            /// Schema version have been checked.
            /// </summary>
            SchemaVersionChecked  = 0,

            /// <summary>
            /// Schema has been migrated
            /// </summary>
            SchemaMigrated = 1,

            /// <summary>
            /// StreamIds loaded
            /// </summary>
            StreamIdsLoaded = 2,

            /// <summary>
            /// Metadata migrated.
            /// </summary>
            MetadataMigrated = 3,
        }
    
        /// <summary>
        ///     The stage 
        /// </summary>
        public MigrateStage Stage { get; }

        internal MigrateProgress(MigrateStage stage)
        {
            Stage = stage;
        }
    }
}