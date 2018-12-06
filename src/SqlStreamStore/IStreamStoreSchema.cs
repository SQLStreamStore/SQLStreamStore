
namespace SqlStreamStore
{
    /// <summary>
    /// Interface to get access to the SQL Creation scripts for each SQL based implementation of SQL Stream Store.
    ///
    /// This can be used when you need to run in an enterprise environment where the process doesn't have access to
    /// do DDL statements and you need to be able to hand the DBA a script to execute. This interface allows you to
    /// export the SQL Script, for example from the command line of your application.
    ///
    /// This is an optional interface that specific implementations of SqlStreamStore can choose to implement.
    /// </summary>
    public interface IStreamStoreSchema
    {
        /// <summary>
        /// Returns the latest version of the SQL Creation script for this specific database implementation. 
        /// </summary>
        /// <returns>The SQL Script that creates the SQL Stream Store Schema</returns>
        string GetSchemaCreationScript();
    }
}
