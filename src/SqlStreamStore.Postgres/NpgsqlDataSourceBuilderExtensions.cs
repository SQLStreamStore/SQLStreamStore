namespace SqlStreamStore
{
    using System;
    using Npgsql;

    public static class NpgsqlDataSourceBuilderExtensions
    {
        /// <summary>
        /// Adds the required composite types to make PostgresStreamStore work
        /// </summary>
        /// <param name="builder"></param>
        /// <returns></returns>
        public static NpgsqlDataSourceBuilder AddPostgresStreamStoreTypes(this NpgsqlDataSourceBuilder builder)
        {
            if(builder == null)
                throw new ArgumentNullException(nameof(builder));
            
            // We can do this as long as we don't have multiple StreamStores over multiple schemas in 1 db
            builder.MapComposite<PostgresNewStreamMessage>(PostgresNewStreamMessage.DataTypeName);
            return builder;
        }
    }
}