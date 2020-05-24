namespace SqlStreamStore.Oracle.Database
{
    using System;
    using System.Data;
    using global::Oracle.ManagedDataAccess.Client;

    internal static class Parameters
    {
        private const int StreamIdSize = 42;
        private const int OriginalStreamIdSize = 1000;

        public static OracleParameter StreamId(OracleStreamId value)
        {
            return new OracleParameter("StreamId", OracleDbType.Char, StreamIdSize, value.Id, ParameterDirection.Input);
        }

        public static OracleParameter StreamIdOriginal(OracleStreamId value)
        {
            return new OracleParameter("StreamIdOriginal", OracleDbType.NVarchar2, OriginalStreamIdSize, value.IdOriginal, ParameterDirection.Input);
        }

        public static OracleParameter MetadataStreamId(OracleStreamId value)
        {
            return new OracleParameter("MetaStreamId", OracleDbType.Char, StreamIdSize, value.Id, ParameterDirection.Input);
        }
        
        public static OracleParameter MetadataStreamIdOriginal(OracleStreamId value)
        {
            return new OracleParameter("MetaStreamIdOriginal", OracleDbType.NVarchar2, OriginalStreamIdSize, value.IdOriginal, ParameterDirection.Input);
        }
        
        public static OracleParameter ExpectedVersion(int value)
        {
            return new OracleParameter("ExpectedVersion", OracleDbType.Int16, value, ParameterDirection.Input);
        }
        
        public static OracleParameter EventId(Guid value)
        {
            return new OracleParameter("EventId", OracleDbType.Varchar2, 40, value.ToString("N"), ParameterDirection.Input);
        }

    }
}