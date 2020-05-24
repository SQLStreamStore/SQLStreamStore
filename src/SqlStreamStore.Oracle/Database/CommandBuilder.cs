namespace SqlStreamStore.Oracle.Database
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Text;
    using global::Oracle.ManagedDataAccess.Client;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Oracle;
    using SqlStreamStore.OracleDatabase;
    using SqlStreamStore.Streams;

    public class CommandBuilder
    {
        private readonly OracleDbObjects _dbObjects;
        private readonly GetUtcNow GetUtcNow;

        public CommandBuilder(OracleDbObjects dbObjects, GetUtcNow getUtcNow)
        {
            _dbObjects = dbObjects;
            GetUtcNow = getUtcNow;
        }

        internal OracleCommand AppendToStreamAnyVersion(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            NewStreamMessage[] messages)
        {
            var parameters = new List<OracleParameter>()
            {
                Parameters.StreamId(streamInfo.StreamId),
                Parameters.StreamIdOriginal(streamInfo.StreamId),
                Parameters.MetadataStreamId(streamInfo.MetaStreamId)
            };
            
            var callFunc = $"{_dbObjects.AppendToStreamAnyVersion}(:StreamId, :MetaStreamId, :StreamIdOriginal, V_NewStreamMessages, :oDeletedEvents)";

            
            return BuildAppend(transaction, callFunc, messages, parameters);
        }
        
        internal OracleCommand AppendToStreamNoStream(
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            NewStreamMessage[] messages)
        {
            var parameters = new List<OracleParameter>()
            {
                Parameters.StreamId(streamInfo.StreamId),
                Parameters.StreamIdOriginal(streamInfo.StreamId),
                Parameters.MetadataStreamId(streamInfo.MetaStreamId)
            };
            
            var callFunc = $"{_dbObjects.AppendToStreamNoStream}(:StreamId, :MetaStreamId, :StreamIdOriginal, V_NewStreamMessages, :oDeletedEvents)";

            
            return BuildAppend(transaction, callFunc, messages, parameters);
        }

        internal OracleCommand AppendToStreamExpectedVersion(
            
            OracleTransaction transaction,
            StreamIdInfo streamInfo,
            int expectedVersion,
            NewStreamMessage[] messages)
        {
            var parameters = new List<OracleParameter>()
            {
                Parameters.ExpectedVersion(expectedVersion),
                Parameters.StreamId(streamInfo.StreamId),
                Parameters.StreamIdOriginal(streamInfo.StreamId),
                Parameters.MetadataStreamId(streamInfo.MetaStreamId)
            };

            var callFunc = $"{_dbObjects.AppendToStreamExpectedVersion}(:StreamId, :ExpectedVersion, V_NewStreamMessages, :oDeletedEvents)";

            return BuildAppend(transaction, callFunc, messages, parameters);
        }

        private OracleCommand BuildAppend(OracleTransaction transaction, string callFunction, NewStreamMessage[] messages, List<OracleParameter> parameters)
        {
            DateTime? dateTime = GetUtcNow?.Invoke();
            
            parameters.Add(new OracleParameter(":created", OracleDbType.TimeStamp, dateTime, ParameterDirection.Input));
            
            var builder = new StringBuilder(@"
                DECLARE 
                    V_NewStreamMessages STREAMNEWMESSAGES := STREAMNEWMESSAGES();
                    V_Result STREAMAPPENDED;
                BEGIN
            ");

            builder.AppendLine($"V_NewStreamMessages.EXTEND({messages.Length});");

            var countParams = 0;
            for(var i=0; i<messages.Length; i++)
            {
                var message = messages[i];
                var messageId = message.MessageId.ToString("N");
                
                var typeParam = countParams++;
                parameters.Add(new OracleParameter(":p" + typeParam, OracleDbType.NVarchar2, 128, message.Type, ParameterDirection.Input));
                
                var jsonParam = countParams++;
                parameters.Add(new OracleParameter(":p" + jsonParam, OracleDbType.NClob, message.JsonData, ParameterDirection.Input));
                
                var jsonMetaParam = (int?) null;
                var jsonMetaParamValue = "NULL";
                if(!string.IsNullOrWhiteSpace(message.JsonMetadata))
                {
                    jsonMetaParam = countParams++;
                    jsonMetaParamValue = ":p" + jsonMetaParam;
                    parameters.Add(new OracleParameter(jsonMetaParamValue, OracleDbType.NClob, message.JsonMetadata, ParameterDirection.Input));
                }
                
                builder.AppendLine($"V_NewStreamMessages({i+1}) := STREAMNEWMESSAGE('{messageId}', :p{typeParam}, :created, :p{jsonParam}, {jsonMetaParamValue}); ");    
            }

            builder.AppendLine($"V_Result := {callFunction};");
            
            parameters.AddRange(new []
            {
                new OracleParameter("oVersion", OracleDbType.Int32, ParameterDirection.Output),
                new OracleParameter("oPosition", OracleDbType.Int64, ParameterDirection.Output),
                new OracleParameter("oDeletedEvents", OracleDbType.RefCursor, ParameterDirection.Output)
            });
            
            builder.AppendLine(":oVersion := V_Result.CURRENTVERSION;");
            builder.AppendLine(":oPosition := V_Result.CURRENTPOSITION;");
            builder.AppendLine("END;");

            var command = new OracleCommand(builder.ToString(), transaction.Connection)
            {
                BindByName = true
            };
            command.Parameters.AddRange(parameters.ToArray());

            return command;
        }

        internal OracleCommand ReadStream(
            
            OracleConnection connection,
            OracleStreamId streamId,
            int count,
            int version,
            bool forward,
            bool prefetch)
        {
            var cmd = new OracleCommand
            {
                Connection = connection,
                BindByName = true,
                CommandText = $"BEGIN {_dbObjects.ReadStream}(:streamId, :count, :version, :forwards, :prefetch, :streamInfo, :streamEvents); END;"
            };

            cmd.Parameters.Add(Parameters.StreamId(streamId));
            cmd.Parameters.Add(new OracleParameter("count", OracleDbType.Int32, count, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("version", OracleDbType.Int32, version, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("forwards", OracleDbType.Int16, forward ? 1 : 0, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("prefetch", OracleDbType.Int16, prefetch ? 1 : 0, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("streamInfo", OracleDbType.RefCursor, ParameterDirection.Output));
            cmd.Parameters.Add(new OracleParameter("streamEvents", OracleDbType.RefCursor, ParameterDirection.Output));

            return cmd;
        }
        
        internal OracleCommand ReadAll(
            
            OracleConnection connection,
            Int64 position,
            int count,
            bool forward,
            bool prefetch)
        {
            var cmd = new OracleCommand
            {
                Connection = connection,
                BindByName = true,
                CommandText = $"BEGIN {_dbObjects.ReadAll}(:position, :count, :forwards, :prefetch, :allEvents); END;"
            };

            cmd.Parameters.Add(new OracleParameter("position", OracleDbType.Int64, position, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("count", OracleDbType.Int32, count, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("forwards", OracleDbType.Int16, forward ? 1 : 0, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("prefetch", OracleDbType.Int16, prefetch ? 1 : 0, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("allEvents", OracleDbType.RefCursor, ParameterDirection.Output));

            return cmd;
        }

        
        internal OracleCommand ReadMessageData(OracleConnection conn,  string streamId, Guid messageId)
        {
            var command = new OracleCommand($"SELECT JsonData FROM {_dbObjects.TableStreamEvents} INNER JOIN {_dbObjects.TableStreams} ON {_dbObjects.TableStreams}.IdInternal = {_dbObjects.TableStreamEvents}.StreamIdInternal WHERE {_dbObjects.TableStreamEvents}.Id = :Id AND {_dbObjects.TableStreams}.Id = :StreamId", conn)
            { 
                BindByName = true
            };
            command.Parameters.Add(new OracleParameter("StreamId", OracleDbType.Char, 40, streamId, ParameterDirection.Input));
            command.Parameters.Add(new OracleParameter("Id", OracleDbType.Varchar2, 40, messageId.ToString("N"), ParameterDirection.Input));

            return command;
        }

        internal OracleCommand DeleteMessage(
            OracleTransaction transaction,
            
            OracleStreamId streamId,
            Guid eventId)
        {
            var cmd = new OracleCommand
            {
                Connection = transaction.Connection,
                BindByName = true,
                CommandText = $"DELETE FROM {_dbObjects.TableStreamEvents} WHERE {_dbObjects.TableStreamEvents}.Id = :EventId AND {_dbObjects.TableStreamEvents}.StreamIdInternal IN (SELECT {_dbObjects.TableStreams}.IdInternal FROM {_dbObjects.TableStreams} WHERE  {_dbObjects.TableStreams}.Id = :StreamId)"
            };

            cmd.Parameters.Add(Parameters.StreamId(streamId));
            cmd.Parameters.Add(Parameters.EventId(eventId));
            
            return cmd;
        }

        internal OracleCommand DeleteStreamAnyVersion(
            OracleTransaction transaction,
            
            StreamIdInfo streamIdInfo)
        {
            var cmd = new OracleCommand
            {
                Connection = transaction.Connection,
                BindByName = true,
                CommandText = $"BEGIN {_dbObjects.DeleteStreamAnyVersion}(:StreamId, :MetaStreamId, :oDeletedStream, :oDeletedMetaStream); END;"
            };
            
            cmd.Parameters.Add(Parameters.StreamId(streamIdInfo.StreamId));
            cmd.Parameters.Add(Parameters.MetadataStreamId(streamIdInfo.MetaStreamId));
            cmd.Parameters.Add(new OracleParameter("oDeletedStream", OracleDbType.Int32, ParameterDirection.Output));
            cmd.Parameters.Add(new OracleParameter("oDeletedMetaStream", OracleDbType.Int32, ParameterDirection.Output));
            
            return cmd;
        }
        
        internal OracleCommand DeleteStreamExpectedVersion(
            OracleTransaction transaction,
            
            StreamIdInfo streamIdInfo,
            int expectedVersion)
        {
            var cmd = new OracleCommand
            {
                Connection = transaction.Connection,
                BindByName = true,
                CommandText = $"BEGIN {_dbObjects.DeleteStreamExpectedVersion}(:StreamId, :MetaStreamId, :ExpectedVersion, :oDeletedStream, :oDeletedMetaStream); END;"
            };
            
            cmd.Parameters.Add(Parameters.StreamId(streamIdInfo.StreamId));
            cmd.Parameters.Add(Parameters.MetadataStreamId(streamIdInfo.MetaStreamId));
            cmd.Parameters.Add(Parameters.ExpectedVersion(expectedVersion));
            cmd.Parameters.Add(new OracleParameter("oDeletedStream", OracleDbType.Int32, ParameterDirection.Output));
            cmd.Parameters.Add(new OracleParameter("oDeletedMetaStream", OracleDbType.Int32, ParameterDirection.Output));
           
            return cmd;
        }

        internal OracleCommand SetMeta(
            OracleTransaction transaction,
            
            StreamIdInfo streamId,
            int? maxAge,
            int? maxCount)
        {
            var cmd = new OracleCommand
            {
                Connection = transaction.Connection,
                BindByName = true,
                CommandText = $"BEGIN {_dbObjects.SetStreamMeta}(:StreamId, :MetaStreamId, :MaxAge, :MaxCount, :oDeletedEvents); END;"
            };
            
            cmd.Parameters.Add(Parameters.StreamId(streamId.StreamId));
            cmd.Parameters.Add(Parameters.MetadataStreamId(streamId.MetaStreamId));
            cmd.Parameters.Add(new OracleParameter("MaxAge", OracleDbType.Int64, maxAge, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("MaxCount", OracleDbType.Int64, maxCount, ParameterDirection.Input));
            cmd.Parameters.Add(new OracleParameter("oDeletedEvents", OracleDbType.RefCursor, ParameterDirection.Output));
            
            return cmd;
        }
    }
}