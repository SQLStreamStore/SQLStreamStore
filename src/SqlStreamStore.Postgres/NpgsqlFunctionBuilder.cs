namespace SqlStreamStore
{
    using System;
    using System.Text;
    using Npgsql;

    internal static class NpgsqlFunctionBuilder
    {
        internal static void BuildFunction(this NpgsqlCommand command)
        {
            if(string.IsNullOrEmpty(command?.CommandText))
                throw new InvalidOperationException("CommandText property has not been initialized");

            var inputList = command.Parameters;
            var numInput = inputList.Count;

            var sb = new StringBuilder();
            sb.Append("SELECT * FROM ");
            sb.Append(command.CommandText);
            sb.Append('(');
            var hasWrittenFirst = false;
            for(var i = 1; i <= numInput; i++)
            {
                if(hasWrittenFirst)
                    sb.Append(',');
                sb.Append('$');
                sb.Append(i);
                hasWrittenFirst = true;
            }

            sb.Append(')');
            command.CommandText = sb.ToString();
        }
    }
}